// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::net::Shutdown;

use common_exception::exception::ABORT_SESSION;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use common_runtime::tokio::net::TcpStream;
use msql_srv::MysqlIntermediary;

use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::sessions::SessionRef;

pub struct MySQLConnection;

impl MySQLConnection {
    pub fn run_on_stream(session: SessionRef, stream: TcpStream) -> Result<()> {
        let blocking_stream = Self::convert_stream(stream)?;
        MySQLConnection::attach_session(&session, &blocking_stream)?;
        std::thread::spawn(move || {
            MySQLConnection::session_executor(session, blocking_stream);
        });

        Ok(())
    }

    fn session_executor(session: SessionRef, blocking_stream: std::net::TcpStream) {
        let interactive_worker = InteractiveWorker::create(session);
        if let Err(error) = MysqlIntermediary::run_on_tcp(interactive_worker, blocking_stream) {
            if error.code() != ABORT_SESSION {
                log::error!(
                    "Unexpected error occurred during query execution: {:?}",
                    error
                );
            }
        };
    }

    fn attach_session(session: &SessionRef, blocking_stream: &std::net::TcpStream) -> Result<()> {
        let host = blocking_stream.peer_addr().ok();
        let blocking_stream_ref = blocking_stream.try_clone()?;
        session.attach(host, move || {
            if let Err(error) = blocking_stream_ref.shutdown(Shutdown::Both) {
                log::error!("Cannot shutdown MySQL session io {}", error);
            }
        });

        Ok(())
    }

    // TODO: move to ToBlockingStream trait
    fn convert_stream(stream: TcpStream) -> Result<std::net::TcpStream> {
        let stream = stream
            .into_std()
            .map_err_to_code(ErrorCode::TokioError, || {
                "Cannot to convert Tokio TcpStream to Std TcpStream"
            })?;
        stream
            .set_nonblocking(false)
            .map_err_to_code(ErrorCode::TokioError, || {
                "Cannot to convert Tokio TcpStream to Std TcpStream"
            })?;

        Ok(stream)
    }
}
