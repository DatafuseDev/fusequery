// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use clickhouse_srv::connection::Connection;
use clickhouse_srv::error_codes::NO_FREE_CONNECTION;
use clickhouse_srv::errors::Error;
use clickhouse_srv::errors::Result as CHResult;
use clickhouse_srv::errors::ServerError;
use clickhouse_srv::protocols::Packet;
use clickhouse_srv::CHContext;
use clickhouse_srv::ClickHouseSession;
use clickhouse_srv::QueryState;
use common_exception::ErrorCode;
use common_exception::Result;
use common_runtime::tokio::net::TcpStream;

pub struct RejectCHConnection;

impl RejectCHConnection {
    pub async fn reject(stream: TcpStream, error: ErrorCode) -> Result<()> {
        let mut ctx = CHContext::new(QueryState::default());

        let dummy_session = DummyCHSession::create();
        match Connection::new(stream, dummy_session, String::from("UTC")) {
            Err(_) => Err(ErrorCode::LogicalError("Cannot create connection")),
            Ok(mut connection) => {
                if let Ok(Some(Packet::Hello(_))) = connection.read_packet(&mut ctx).await {
                    let server_error = Error::Server(ServerError {
                        code: NO_FREE_CONNECTION,
                        name: String::from("NO_FREE_CONNECTION"),
                        message: error.message(),
                        stack_trace: String::from(""),
                    });
                    let _ = connection.write_error(&server_error).await;
                }

                Ok(())
            }
        }
    }
}

struct DummyCHSession;

impl DummyCHSession {
    pub fn create() -> Arc<dyn ClickHouseSession> {
        Arc::new(DummyCHSession {})
    }
}

#[async_trait::async_trait]
impl ClickHouseSession for DummyCHSession {
    async fn execute_query(&self, _: &mut CHContext, _: &mut Connection) -> CHResult<()> {
        unimplemented!()
    }
}
