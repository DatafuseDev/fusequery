// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::headers::HEADER_DEDUPLICATE_LABEL;
use databend_common_base::headers::HEADER_NODE_ID;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_SESSION_ID;
use databend_common_base::headers::HEADER_TENANT;
use databend_common_base::headers::HEADER_VERSION;
use databend_common_base::runtime::ThreadTracker;
use databend_common_config::GlobalConfig;
use databend_common_config::QUERY_SEMVER;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::user_token::TokenType;
use databend_common_meta_app::tenant::Tenant;
use fastrace::func_name;
use headers::authorization::Basic;
use headers::authorization::Bearer;
use headers::authorization::Credentials;
use http::header::AUTHORIZATION;
use http::HeaderMap;
use http::HeaderValue;
use http::StatusCode;
use log::error;
use log::warn;
use opentelemetry::baggage::BaggageExt;
use opentelemetry::propagation::Extractor;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::BaggagePropagator;
use poem::error::ResponseError;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::Addr;
use poem::Endpoint;
use poem::IntoResponse;
use poem::Middleware;
use poem::Request;
use poem::Response;
use uuid::Uuid;

use crate::auth::AuthMgr;
use crate::auth::Credential;
use crate::servers::http::error::HttpErrorCode;
use crate::servers::http::error::JsonErrorOnly;
use crate::servers::http::error::QueryError;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::SessionClaim;
use crate::servers::HttpHandlerKind;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
const USER_AGENT: &str = "User-Agent";
const TRACE_PARENT: &str = "traceparent";
#[derive(Debug, Copy, Clone)]
pub enum EndpointKind {
    Login,
    Logout,
    Refresh,
    StartQuery,
    PollQuery,
    Clickhouse,
    NoAuth,
    Verify,
}

impl EndpointKind {
    /// avoid the cost of get user from meta
    pub fn need_user_info(&self) -> bool {
        !matches!(self, EndpointKind::NoAuth | EndpointKind::PollQuery)
    }
    pub fn require_databend_token_type(&self) -> Result<Option<TokenType>> {
        match self {
            EndpointKind::Verify => Ok(None),
            EndpointKind::Refresh => Ok(Some(TokenType::Refresh)),
            EndpointKind::StartQuery | EndpointKind::PollQuery | EndpointKind::Logout => {
                if GlobalConfig::instance().query.management_mode {
                    Ok(None)
                } else {
                    Ok(Some(TokenType::Session))
                }
            }
            _ => Err(ErrorCode::AuthenticateFailure(format!(
                "should not use databend token for {self:?}",
            ))),
        }
    }
}

pub struct HTTPSessionMiddleware {
    pub kind: HttpHandlerKind,
    pub endpoint_kind: EndpointKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl HTTPSessionMiddleware {
    pub fn create(kind: HttpHandlerKind, endpoint_kind: EndpointKind) -> HTTPSessionMiddleware {
        let auth_manager = AuthMgr::instance();
        HTTPSessionMiddleware {
            kind,
            endpoint_kind,
            auth_manager,
        }
    }
}

pub struct HeaderExtractor<'a>(pub &'a http::HeaderMap);
impl<'a> Extractor for HeaderExtractor<'a> {
    /// Get a value for a key from the HeaderMap.  If the value is not valid ASCII, returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    /// Collect all the keys from the HeaderMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
    }
}

fn extract_baggage_from_headers(headers: &HeaderMap) -> Option<Vec<(String, String)>> {
    headers.get("baggage")?;
    let propagator = BaggagePropagator::new();
    let extractor = HeaderExtractor(headers);
    let result: Vec<(String, String)> = {
        let context = propagator.extract(&extractor);
        let baggage = context.baggage();
        baggage
            .iter()
            .map(|(key, (value, _metadata))| (key.to_string(), value.to_string()))
            .collect()
    };
    Some(result)
}

fn get_credential(
    req: &Request,
    kind: HttpHandlerKind,
    endpoint_kind: EndpointKind,
) -> Result<Credential> {
    if matches!(endpoint_kind, EndpointKind::NoAuth) {
        return Ok(Credential::NoNeed);
    }
    let std_auth_headers: Vec<_> = req.headers().get_all(AUTHORIZATION).iter().collect();
    if std_auth_headers.len() > 1 {
        let msg = &format!("Multiple {} headers detected", AUTHORIZATION);
        return Err(ErrorCode::AuthenticateFailure(msg));
    }
    let client_ip = get_client_ip(req);
    if std_auth_headers.is_empty() {
        if matches!(kind, HttpHandlerKind::Clickhouse) {
            get_clickhouse_name_password(req, client_ip)
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "No authorization header detected",
            ))
        }
    } else {
        get_credential_from_header(&std_auth_headers, client_ip, endpoint_kind)
    }
}

/// this function tries to get the client IP address from the headers. if the ip in header
/// not found, fallback to the remote address, which might be local proxy's ip address.
/// please note that when it comes with network policy, we need make sure the incoming
/// traffic comes from a trustworthy proxy instance.
fn get_client_ip(req: &Request) -> Option<String> {
    let headers = ["X-Real-IP", "X-Forwarded-For", "CF-Connecting-IP"];
    for &header in headers.iter() {
        if let Some(value) = req.headers().get(header) {
            if let Ok(mut ip_str) = value.to_str() {
                if header == "X-Forwarded-For" {
                    ip_str = ip_str.split(',').next().unwrap_or("");
                }
                return Some(ip_str.to_string());
            }
        }
    }

    // fallback to the connection's remote address, take care
    let client_ip = match req.remote_addr().0 {
        Addr::SocketAddr(addr) => Some(addr.ip().to_string()),
        Addr::Custom(..) => Some("127.0.0.1".to_string()),
        _ => None,
    };

    client_ip
}

fn get_credential_from_header(
    std_auth_headers: &[&HeaderValue],
    client_ip: Option<String>,
    endpoint_kind: EndpointKind,
) -> Result<Credential> {
    let value = &std_auth_headers[0];
    if value.as_bytes().starts_with(b"Basic ") {
        match Basic::decode(value) {
            Some(basic) => {
                let name = basic.username().to_string();
                let password = basic.password().to_owned().as_bytes().to_vec();
                let password = (!password.is_empty()).then_some(password);
                let c = Credential::Password {
                    name,
                    password,
                    client_ip,
                };
                Ok(c)
            }
            None => Err(ErrorCode::AuthenticateFailure("bad Basic auth header")),
        }
    } else if value.as_bytes().starts_with(b"Bearer ") {
        match Bearer::decode(value) {
            Some(bearer) => {
                let token = bearer.token().to_string();
                if SessionClaim::is_databend_token(&token) {
                    if let Some(t) = endpoint_kind.require_databend_token_type()? {
                        if t != SessionClaim::get_type(&token)? {
                            return Err(ErrorCode::AuthenticateFailure("wrong data token type"));
                        }
                    }
                    Ok(Credential::DatabendToken { token })
                } else {
                    Ok(Credential::Jwt { token, client_ip })
                }
            }
            None => Err(ErrorCode::AuthenticateFailure("bad Bearer auth header")),
        }
    } else {
        Err(ErrorCode::AuthenticateFailure("bad auth header"))
    }
}

fn get_clickhouse_name_password(req: &Request, client_ip: Option<String>) -> Result<Credential> {
    let (user, key) = (
        req.headers().get("X-CLICKHOUSE-USER"),
        req.headers().get("X-CLICKHOUSE-KEY"),
    );
    if let (Some(name), Some(password)) = (user, key) {
        let c = Credential::Password {
            name: String::from_utf8(name.as_bytes().to_vec()).unwrap(),
            password: Some(password.as_bytes().to_vec()),
            client_ip,
        };
        Ok(c)
    } else {
        let query_str = req.uri().query().unwrap_or_default();
        let query_params = serde_urlencoded::from_str::<HashMap<String, String>>(query_str)
            .map_err(|e| ErrorCode::BadArguments(format!("{}", e)))?;
        let (user, key) = (query_params.get("user"), query_params.get("password"));
        if let (Some(name), Some(password)) = (user, key) {
            Ok(Credential::Password {
                name: name.clone(),
                password: Some(password.as_bytes().to_vec()),
                client_ip,
            })
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "No header or query parameters for authorization detected",
            ))
        }
    }
}

impl<E: Endpoint> Middleware<E> for HTTPSessionMiddleware {
    type Output = HTTPSessionEndpoint<E>;
    fn transform(&self, ep: E) -> Self::Output {
        HTTPSessionEndpoint {
            ep,
            kind: self.kind,
            endpoint_kind: self.endpoint_kind,
            auth_manager: self.auth_manager.clone(),
        }
    }
}

pub struct HTTPSessionEndpoint<E> {
    ep: E,
    pub kind: HttpHandlerKind,
    pub endpoint_kind: EndpointKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl<E> HTTPSessionEndpoint<E> {
    #[async_backtrace::framed]
    async fn auth(&self, req: &Request, query_id: String) -> Result<HttpQueryContext> {
        let credential = get_credential(req, self.kind, self.endpoint_kind)?;

        let session_manager = SessionManager::instance();

        let mut session = session_manager.create_session(SessionType::Dummy).await?;

        if let Some(tenant_id) = req.headers().get(HEADER_TENANT) {
            let tenant_id = tenant_id.to_str().unwrap().to_string();
            let tenant = Tenant::new_or_err(tenant_id.clone(), func_name!())?;
            session.set_current_tenant(tenant);
        }

        let header_client_session_id = req
            .headers()
            .get(HEADER_SESSION_ID)
            .map(|v| v.to_str().unwrap().to_string());
        let (user_name, authed_client_session_id) = self
            .auth_manager
            .auth(
                &mut session,
                &credential,
                self.endpoint_kind.need_user_info(),
            )
            .await?;
        let client_session_id = authed_client_session_id.or(header_client_session_id);
        if let Some(id) = client_session_id.clone() {
            session.set_client_session_id(id)
        }

        let session = session_manager.register_session(session)?;

        let deduplicate_label = req
            .headers()
            .get(HEADER_DEDUPLICATE_LABEL)
            .map(|id| id.to_str().unwrap().to_string());

        let user_agent = req
            .headers()
            .get(USER_AGENT)
            .map(|id| id.to_str().unwrap().to_string());

        let expected_node_id = req
            .headers()
            .get(HEADER_NODE_ID)
            .map(|id| id.to_str().unwrap().to_string());

        let trace_parent = req
            .headers()
            .get(TRACE_PARENT)
            .map(|id| id.to_str().unwrap().to_string());
        let opentelemetry_baggage = extract_baggage_from_headers(req.headers());
        let client_host = get_client_ip(req);

        let node_id = GlobalConfig::instance().query.node_id.clone();

        Ok(HttpQueryContext {
            session,
            query_id,
            node_id,
            credential,
            expected_node_id,
            deduplicate_label,
            user_agent,
            trace_parent,
            opentelemetry_baggage,
            http_method: req.method().to_string(),
            uri: req.uri().to_string(),
            client_host,
            client_session_id,
            user_name,
        })
    }
}

impl<E: Endpoint> Endpoint for HTTPSessionEndpoint<E> {
    type Output = Response;

    #[async_backtrace::framed]
    async fn call(&self, mut req: Request) -> PoemResult<Self::Output> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();

        let query_id = req
            .headers()
            .get(HEADER_QUERY_ID)
            .map(|id| id.to_str().unwrap().to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.query_id = Some(query_id.clone());
        let _guard = ThreadTracker::tracking(tracking_payload);

        ThreadTracker::tracking_future(async move {
            match self.auth(&req, query_id).await {
                Ok(ctx) => {
                    req.extensions_mut().insert(ctx);
                    self.ep.call(req).await.map(|v| v.into_response())
                }
                Err(err) => {
                    let err = HttpErrorCode::error_code(err);
                    if err.status() == StatusCode::UNAUTHORIZED {
                        warn!(
                            "http auth failure: {method} {uri}, headers={:?}, error={}",
                            sanitize_request_headers(&headers),
                            err
                        );
                    } else {
                        error!(
                            "http request err: {method} {uri}, headers={:?}, error={}",
                            sanitize_request_headers(&headers),
                            err
                        );
                    }
                    Ok(err.as_response())
                }
            }
        })
        .await
    }
}

pub fn sanitize_request_headers(headers: &poem::http::HeaderMap) -> HashMap<String, String> {
    let sensitive_headers = ["authorization", "x-clickhouse-key", "cookie"];
    headers
        .iter()
        .map(|(k, v)| {
            let k = k.as_str().to_lowercase();
            if sensitive_headers.contains(&k.as_str()) {
                (k, "******".to_string())
            } else {
                (k, v.to_str().unwrap_or_default().to_string())
            }
        })
        .collect()
}

pub async fn json_response<E: Endpoint>(next: E, req: Request) -> PoemResult<Response> {
    let mut resp = match next.call(req).await {
        Ok(resp) => resp.into_response(),
        Err(err) => (
            err.status(),
            Json(JsonErrorOnly {
                error: QueryError {
                    code: err.status().as_u16(),
                    message: err.to_string(),
                    detail: None,
                },
            }),
        )
            .into_response(),
    };
    resp.headers_mut()
        .insert(HEADER_VERSION, QUERY_SEMVER.to_string().parse().unwrap());
    Ok(resp)
}
