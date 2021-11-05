// Copyright 2020 Datafuse Labs.
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

use std::net::SocketAddr;

use common_exception::ErrorCode;
use common_exception::Result;
use common_metrics::PrometheusHandle;
use poem::web::Data;
use poem::web::Html;
use poem::EndpointExt;
use poem::IntoResponse;
use poem::Response;

use crate::common::service::HttpShutdownHandler;
use crate::servers::Server;
use crate::sessions::SessionManagerRef;

pub struct MetricService {
    shutdown_handler: HttpShutdownHandler,
}

pub struct MetricTemplate {
    prom: PrometheusHandle,
}

impl IntoResponse for MetricTemplate {
    fn into_response(self) -> Response {
        Html(self.prom.render()).into_response()
    }
}

#[poem::handler]
pub async fn metric_handler(prom_extension: Data<&PrometheusHandle>) -> MetricTemplate {
    let prom = prom_extension.0.clone();
    MetricTemplate { prom }
}

impl MetricService {
    // TODO add session tls handler
    pub fn create(_sessions: SessionManagerRef) -> Box<MetricService> {
        Box::new(MetricService {
            shutdown_handler: HttpShutdownHandler::create("metric api".to_string()),
        })
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        let prometheus_handle = common_metrics::try_handle().ok_or_else(|| {
            ErrorCode::InitPrometheusFailure("Prometheus recorder has not been initialized yet.")
        })?;
        let app = poem::Route::new()
            .at("/metrics", poem::get(metric_handler))
            .data(prometheus_handle);
        let addr = self
            .shutdown_handler
            .start_service(listening, None, app)
            .await?;
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl Server for MetricService {
    async fn shutdown(&mut self, graceful: bool) {
        self.shutdown_handler.shutdown(graceful).await;
    }

    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        self.start_without_tls(listening).await
    }
}
