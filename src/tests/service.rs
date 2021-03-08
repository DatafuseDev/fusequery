// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use rand::Rng;

use crate::clusters::Cluster;
use crate::configs::Config;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::rpcs::RpcService;
use crate::sessions::Session;

/// Start services and return the random address.
pub async fn try_start_service() -> FuseQueryResult<String> {
    let mut rng = rand::thread_rng();
    let port: u32 = rng.gen_range(10000..11000);
    let addr = format!("127.0.0.1:{}", port);

    let mut conf = Config::default();
    conf.rpc_api_address = addr.clone();

    let cluster = Cluster::create(conf.clone());
    let session_manager = Session::create();
    let srv = RpcService::create(conf.clone(), cluster.clone(), session_manager.clone());
    tokio::spawn(async move {
        srv.make_server().await?;
        Ok::<(), FuseQueryError>(())
    });
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

    Ok(addr.clone())
}
