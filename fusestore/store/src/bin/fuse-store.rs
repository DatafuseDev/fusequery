// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_runtime::tokio;
use common_tracing::init_tracing_with_file;
use fuse_store::api::StoreServer;
use fuse_store::configs::Config;
use fuse_store::metrics::MetricService;
use log::info;
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conf = Config::from_args();
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log_level.to_lowercase().as_str()),
    )
    .init();

    let _guards =
        init_tracing_with_file("fuse-store", conf.log_dir.as_str(), conf.log_level.as_str());

    info!("{:?}", conf.clone());
    info!(
        "FuseStore v-{}",
        *fuse_store::configs::config::FUSE_COMMIT_VERSION
    );

    // Metric API service.
    {
        let srv = MetricService::create(conf.clone());
        tokio::spawn(async move {
            srv.make_server().expect("Metrics service error");
        });
        info!("Metric API server listening on {}", conf.metric_api_address);
    }

    // RPC API service.
    {
        let srv = StoreServer::create(conf.clone());
        info!("RPC API server listening on {}", conf.flight_api_address);
        srv.serve().await.expect("RPC service error");
    }

    Ok(())
}
