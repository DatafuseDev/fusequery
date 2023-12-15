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

use databend_common_base::base::mask_connection_info;
use databend_common_exception::ErrorCode;
use databend_common_expression::DataSchemaRef;
use databend_common_metrics::http::metrics_incr_http_response_errors_count;
use highway::HighwayHash;
use log::error;
use log::info;
use minitrace::full_name;
use minitrace::prelude::*;
use poem::error::Error as PoemError;
use poem::error::Result as PoemResult;
use poem::get;
use poem::http::StatusCode;
use poem::post;
use poem::web::Json;
use poem::web::Path;
use poem::EndpointExt;
use poem::IntoResponse;
use poem::Route;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value as JsonValue;

use super::query::ExecuteStateKind;
use super::query::HttpQueryRequest;
use super::query::HttpQueryResponseInternal;
use super::query::RemoveReason;
use crate::servers::http::middleware::MetricsMiddleware;
use crate::servers::http::v1::query::Progresses;
use crate::servers::http::v1::HttpQueryContext;
use crate::servers::http::v1::HttpQueryManager;
use crate::servers::http::v1::HttpSessionConf;
use crate::servers::http::v1::JsonBlock;
use crate::sessions::QueryAffect;

const HEADER_QUERY_ID: &str = "X-DATABEND-QUERY-ID";
const HEADER_QUERY_STATE: &str = "X-DATABEND-QUERY-STATE";
const HEADER_QUERY_PAGE_ROWS: &str = "X-DATABEND-QUERY-PAGE-ROWS";

pub fn make_page_uri(query_id: &str, page_no: usize) -> String {
    format!("/v1/query/{}/page/{}", query_id, page_no)
}

pub fn make_state_uri(query_id: &str) -> String {
    format!("/v1/query/{}", query_id)
}

pub fn make_final_uri(query_id: &str) -> String {
    format!("/v1/query/{}/final", query_id)
}

pub fn make_kill_uri(query_id: &str) -> String {
    format!("/v1/query/{}/kill", query_id)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryError {
    pub code: u16,
    pub message: String,
    pub detail: String,
}

impl QueryError {
    fn from_error_code(e: &ErrorCode) -> Self {
        QueryError {
            code: e.code(),
            message: e.display_text(),
            detail: e.detail(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct QueryStats {
    #[serde(flatten)]
    pub progresses: Progresses,
    pub running_time_ms: i64,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryResponseField {
    name: String,
    r#type: String,
}

impl QueryResponseField {
    fn from_schema(schema: DataSchemaRef) -> Vec<Self> {
        schema
            .fields()
            .iter()
            .map(|f| Self {
                name: f.name().to_string(),
                r#type: f.data_type().wrapped_display(),
            })
            .collect()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct QueryResponse {
    pub id: String,
    pub session_id: Option<String>,
    pub node_id: String,
    pub session: Option<HttpSessionConf>,
    pub schema: Vec<QueryResponseField>,
    pub data: Vec<Vec<JsonValue>>,
    pub state: ExecuteStateKind,
    // only sql query error
    pub error: Option<QueryError>,
    pub stats: QueryStats,
    pub affect: Option<QueryAffect>,
    pub warnings: Vec<String>,
    pub stats_uri: Option<String>,
    // just call it after client not use it anymore, not care about the server-side behavior
    pub final_uri: Option<String>,
    pub next_uri: Option<String>,
    pub kill_uri: Option<String>,
}

impl QueryResponse {
    pub(crate) fn from_internal(
        id: String,
        r: HttpQueryResponseInternal,
        is_final: bool,
    ) -> impl IntoResponse {
        let state = r.state.clone();
        let (data, next_uri) = if is_final {
            (JsonBlock::empty(), None)
        } else {
            match state.state {
                ExecuteStateKind::Running => match r.data {
                    None => (JsonBlock::empty(), Some(make_state_uri(&id))),
                    Some(d) => {
                        let uri = match d.next_page_no {
                            Some(n) => Some(make_page_uri(&id, n)),
                            None => Some(make_state_uri(&id)),
                        };
                        (d.page.data, uri)
                    }
                },
                ExecuteStateKind::Failed => (JsonBlock::empty(), Some(make_final_uri(&id))),
                ExecuteStateKind::Succeeded => match r.data {
                    None => (JsonBlock::empty(), Some(make_final_uri(&id))),
                    Some(d) => {
                        let uri = match d.next_page_no {
                            Some(n) => Some(make_page_uri(&id, n)),
                            None => Some(make_final_uri(&id)),
                        };
                        (d.page.data, uri)
                    }
                },
            }
        };

        if let Some(err) = &r.state.error {
            metrics_incr_http_response_errors_count(err.name(), err.code());
        }

        let schema = data.schema().clone();
        let session_id = r.session_id.clone();
        let stats = QueryStats {
            progresses: state.progresses.clone(),
            running_time_ms: state.running_time_ms,
        };
        let rows = data.data.len();

        Json(QueryResponse {
            data: data.into(),
            state: state.state,
            schema: QueryResponseField::from_schema(schema),
            session_id: Some(session_id),
            node_id: r.node_id,
            session: r.session,
            stats,
            affect: state.affect,
            warnings: r.state.warnings,
            id: id.clone(),
            next_uri,
            stats_uri: Some(make_state_uri(&id)),
            final_uri: Some(make_final_uri(&id)),
            kill_uri: Some(make_kill_uri(&id)),
            error: r.state.error.as_ref().map(QueryError::from_error_code),
        })
        .with_header(HEADER_QUERY_ID, id.clone())
        .with_header(HEADER_QUERY_STATE, state.state.to_string())
        .with_header(HEADER_QUERY_PAGE_ROWS, rows)
    }

    pub(crate) fn fail_to_start_sql(err: &ErrorCode) -> impl IntoResponse {
        metrics_incr_http_response_errors_count(err.name(), err.code());
        Json(QueryResponse {
            id: "".to_string(),
            stats: QueryStats::default(),
            state: ExecuteStateKind::Failed,
            affect: None,
            data: vec![],
            schema: vec![],
            session_id: None,
            warnings: vec![],
            node_id: "".to_string(),
            session: None,
            next_uri: None,
            stats_uri: None,
            final_uri: None,
            kill_uri: None,
            error: Some(QueryError::from_error_code(err)),
        })
    }
}

#[poem::handler]
async fn query_final_handler(
    ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    let trace_id = query_id_to_trace_id(&query_id);
    let root = Span::root(
        full_name!(),
        SpanContext::new(trace_id, SpanId(rand::random())),
    );

    async {
        info!("{}: final http query", query_id);
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager
            .remove_query(&query_id, RemoveReason::Finished)
            .await
        {
            Ok(query) => {
                let mut response = query.get_response_state_only().await;
                if response.state.state == ExecuteStateKind::Running {
                    return Err(PoemError::from_string(
                        format!("query {} is still running, can not final it", query_id),
                        StatusCode::BAD_REQUEST,
                    ));
                }
                Ok(QueryResponse::from_internal(query_id, response, true))
            }
            Err(reason) => Err(query_id_not_found_or_removed(
                &query_id,
                &ctx.node_id,
                reason,
            )),
        }
    }
    .in_span(root)
    .await
}

// currently implementation only support kill http query
#[poem::handler]
async fn query_cancel_handler(
    ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    let trace_id = query_id_to_trace_id(&query_id);
    let root = Span::root(
        full_name!(),
        SpanContext::new(trace_id, SpanId(rand::random())),
    );

    async {
        info!("{}: http query is killed", query_id);
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager.try_get_query(&query_id).await {
            Ok(query) => {
                query.kill("http query cancel by handler").await;
                http_query_manager
                    .remove_query(&query_id, RemoveReason::Canceled)
                    .await
                    .ok();
                Ok(StatusCode::OK)
            }
            Err(reason) => Err(query_id_not_found_or_removed(
                &query_id,
                &ctx.node_id,
                reason,
            )),
        }
    }
    .in_span(root)
    .await
}

#[poem::handler]
async fn query_state_handler(
    ctx: &HttpQueryContext,
    Path(query_id): Path<String>,
) -> PoemResult<impl IntoResponse> {
    let trace_id = query_id_to_trace_id(&query_id);
    let root = Span::root(
        full_name!(),
        SpanContext::new(trace_id, SpanId(rand::random())),
    );

    async {
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager.try_get_query(&query_id).await {
            Ok(query) => {
                let response = query.get_response_state_only().await;
                Ok(QueryResponse::from_internal(query_id, response, false))
            }
            Err(reason) => Err(query_id_not_found_or_removed(
                &query_id,
                &ctx.node_id,
                reason,
            )),
        }
    }
    .in_span(root)
    .await
}

#[poem::handler]
async fn query_page_handler(
    ctx: &HttpQueryContext,
    Path((query_id, page_no)): Path<(String, usize)>,
) -> PoemResult<impl IntoResponse> {
    let trace_id = query_id_to_trace_id(&query_id);
    let root = Span::root(
        full_name!(),
        SpanContext::new(trace_id, SpanId(rand::random())),
    );

    async {
        let http_query_manager = HttpQueryManager::instance();
        match http_query_manager.try_get_query(&query_id).await {
            Ok(query) => {
                query.update_expire_time(true).await;
                let resp = query.get_response_page(page_no).await.map_err(|err| {
                    poem::Error::from_string(err.message(), StatusCode::NOT_FOUND)
                })?;
                query.update_expire_time(false).await;
                Ok(QueryResponse::from_internal(query_id, resp, false))
            }
            Err(reason) => Err(query_id_not_found_or_removed(
                &query_id,
                &ctx.node_id,
                reason,
            )),
        }
    }
    .in_span(root)
    .await
}

#[poem::handler]
#[async_backtrace::framed]
pub(crate) async fn query_handler(
    ctx: &HttpQueryContext,
    Json(req): Json<HttpQueryRequest>,
) -> PoemResult<impl IntoResponse> {
    let trace_id = query_id_to_trace_id(&ctx.query_id);
    let root = Span::root(full_name!(), SpanContext::new(trace_id, SpanId::default()));

    async {
        info!("http query new request: {:}", mask_connection_info(&format!("{:?}", req)));
        let http_query_manager = HttpQueryManager::instance();
        let sql = req.sql.clone();

        let query = http_query_manager
            .try_create_query(ctx, req)
            .await
            .map_err(|err| err.display_with_sql(&sql));
        match query {
            Ok(query) => {
                query.update_expire_time(true).await;
                let resp = query
                    .get_response_page(0)
                    .await
                    .map_err(|err| err.display_with_sql(&sql))
                    .map_err(|err| poem::Error::from_string(err.message(), StatusCode::NOT_FOUND))?;
                let (rows, next_page) = match &resp.data {
                    None => (0, None),
                    Some(p) => (p.page.data.num_rows(), p.next_page_no),
                };
                info!(
                    "http query initial response to http query_id={}, state={:?}, rows={}, next_page={:?}, sql='{}'",
                    &query.id, &resp.state, rows, next_page, mask_connection_info(&sql)
                );
                query.update_expire_time(false).await;
                Ok(QueryResponse::from_internal(query.id.to_string(), resp, false).into_response())
            }
            Err(e) => {
                error!("{}: http query fail to start sql, error: {:?}", &ctx.query_id, e);
                Ok(QueryResponse::fail_to_start_sql(&e).into_response())
            }
        }
    }
    .in_span(root)
    .await
}

pub fn query_route() -> Route {
    // Note: endpoints except /v1/query may change without notice, use uris in response instead
    let rules = [
        ("/", post(query_handler)),
        ("/:id", get(query_state_handler)),
        ("/:id/page/:page_no", get(query_page_handler)),
        (
            "/:id/kill",
            get(query_cancel_handler).post(query_cancel_handler),
        ),
        (
            "/:id/final",
            get(query_final_handler).post(query_final_handler),
        ),
    ];

    let mut route = Route::new();
    for (path, endpoint) in rules.into_iter() {
        route = route.at(path, endpoint.with(MetricsMiddleware::new(path)));
    }
    route
}

fn query_id_not_found_or_removed(
    query_id: &str,
    node_id: &str,
    reason: Option<RemoveReason>,
) -> PoemError {
    let error = match reason {
        Some(reason) => reason.to_string(),
        None => "not found".to_string(),
    };
    PoemError::from_string(
        format!("query id {query_id} {error} on {node_id}"),
        StatusCode::NOT_FOUND,
    )
}

fn query_id_to_trace_id(query_id: &str) -> TraceId {
    let [hash_high, hash_low] = highway::PortableHash::default().hash128(query_id.as_bytes());
    TraceId(((hash_high as u128) << 64) + (hash_low as u128))
}
