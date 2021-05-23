// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use fuse_query::api::HttpService;
use fuse_query::api::RpcService;
use fuse_query::clusters::Cluster;
use fuse_query::configs::Config;
use fuse_query::metrics::MetricService;
use fuse_query::servers::ClickHouseHandler;
use fuse_query::servers::MySQLHandler;
use fuse_query::sessions::SessionManager;
use log::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // First load configs from args.
    let mut conf = Config::load_from_args();

    // If config file is not empty: -c xx.toml
    // Reload configs from the file.
    if !conf.config_file.is_empty() {
        info!("Config reload from {:?}", conf.config_file);
        conf = Config::load_from_toml(conf.config_file.as_str())?;
    }

    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or(conf.log_level.to_lowercase().as_str())
    )
    .init();

    info!("{:?}", conf.clone());
    info!("FuseQuery v-{}", conf.version);

    let cluster = Cluster::create(conf.clone());
    let session_manager = SessionManager::create();

    // MySQL handler.
    {
        let handler = MySQLHandler::create(conf.clone(), cluster.clone(), session_manager.clone());
        tokio::spawn(async move { handler.start().expect("MySQL handler error") });

        info!(
            "MySQL handler listening on {}:{}, Usage: mysql -h{} -P{}",
            conf.mysql_handler_host,
            conf.mysql_handler_port,
            conf.mysql_handler_host,
            conf.mysql_handler_port
        );
    }

    // ClickHouse handler.
    {
        let handler =
            ClickHouseHandler::create(conf.clone(), cluster.clone(), session_manager.clone());

        tokio::spawn(async move {
            handler.start().await.expect("ClickHouse handler error");
        });

        info!(
            "ClickHouse handler listening on {}:{}, Usage: clickhouse-client --host {} --port {}",
            conf.clickhouse_handler_host,
            conf.clickhouse_handler_port,
            conf.clickhouse_handler_host,
            conf.clickhouse_handler_port
        );
    }

    // Metric API service.
    {
        let srv = MetricService::create(conf.clone());
        tokio::spawn(async move {
            srv.make_server().expect("Metrics service error");
        });
        info!("Metric API server listening on {}", conf.metric_api_address);
    }

    // HTTP API service.
    {
        let srv = HttpService::create(conf.clone(), cluster.clone());
        tokio::spawn(async move {
            srv.make_server().await.expect("HTTP service error");
        });
        info!("HTTP API server listening on {}", conf.http_api_address);
    }

    // RPC API service.
    {
        let srv = RpcService::create(conf.clone(), cluster.clone(), session_manager.clone());
        info!("RPC API server listening on {}", conf.flight_api_address);
        srv.make_server().await.expect("RPC service error");
    }

    Ok(())
}
