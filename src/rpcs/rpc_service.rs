// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use tonic::transport::Server;

use crate::clusters::ClusterRef;
use crate::configs::Config;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::rpcs::rpc::{ExecutorRPCService, FlightService};
use crate::sessions::SessionRef;

pub struct RpcService {
    conf: Config,
    cluster: ClusterRef,
    session_manager: SessionRef,
}

impl RpcService {
    pub fn create(conf: Config, cluster: ClusterRef, session_manager: SessionRef) -> Self {
        Self {
            conf,
            cluster,
            session_manager,
        }
    }

    pub async fn make_server(&self) -> FuseQueryResult<()> {
        let addr = self.conf.rpc_api_address.parse::<std::net::SocketAddr>()?;

        // GRPC service.
        let grpc_srv = ExecutorRPCService::create();

        // Flight service:
        // For distributed execute engine api.
        let flight_srv = FlightService::create(
            self.conf.clone(),
            self.cluster.clone(),
            self.session_manager.clone(),
        );

        Server::builder()
            .add_service(grpc_srv.make_server())
            .add_service(flight_srv.make_server())
            .serve(addr)
            .await
            .map_err(|e| {
                FuseQueryError::Internal(format!("Metrics prometheus exporter error: {:?}", e))
            })
    }
}
