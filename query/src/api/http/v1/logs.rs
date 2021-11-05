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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_exception::Result;
use common_streams::SendableDataBlockStream;
use poem::http::StatusCode;
use poem::web::Data;
use poem::web::Html;
use poem::IntoResponse;
use poem::Response;
use tokio_stream::StreamExt;

use crate::catalogs::ToReadDataSourcePlan;
use crate::sessions::DatabendQueryContextRef;
use crate::sessions::SessionManagerRef;

pub struct LogTemplate {
    result: Result<String>,
}

impl IntoResponse for LogTemplate {
    fn into_response(self) -> Response {
        match self.result {
            Ok(log) => Html(log).into_response(),
            Err(err) => Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(format!("Failed to fetch log. Error: {}", err)),
        }
    }
}

// read log files from cfg.log.log_dir
#[poem::handler]
pub async fn logs_handler(sessions_extension: Data<&SessionManagerRef>) -> LogTemplate {
    let sessions = sessions_extension.0;
    LogTemplate {
        result: select_table(sessions.clone()).await,
    }
}

async fn select_table(sessions: SessionManagerRef) -> Result<String> {
    let session = sessions.create_session("WatchLogs")?;
    let query_context = session.create_context().await?;

    let tracing_table_stream = execute_query(query_context).await?;
    let tracing_logs = tracing_table_stream
        .collect::<Result<Vec<DataBlock>>>()
        .await?;
    Ok(format!("{:?}", tracing_logs))
}

async fn execute_query(context: DatabendQueryContextRef) -> Result<SendableDataBlockStream> {
    let tracing_table = context.get_table("system", "tracing")?;
    let io_ctx = context.get_cluster_table_io_context()?;
    let io_ctx = Arc::new(io_ctx);

    let tracing_table_read_plan = tracing_table.read_plan(io_ctx.clone(), None)?;

    tracing_table.read(io_ctx, &tracing_table_read_plan).await
}
