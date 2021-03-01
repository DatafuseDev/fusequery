// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use log::info;
use simplelog::{Config as LogConfig, LevelFilter, SimpleLogger};

use tokio::signal::unix::{signal, SignalKind};
use tonic::transport::Server;
use mimalloc::MiMalloc;

use fuse_query::admins::Admin;
use fuse_query::clusters::Cluster;
use fuse_query::configs::Config;
use fuse_query::executors::ExecutorRPCServer;
use fuse_query::metrics::Metric;
use fuse_query::proto::executor_server::ExecutorServer;
use fuse_query::servers::MySQLHandler;
use fuse_query::sessions::Session;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Config::create();

    // Log level.
    match cfg.log_level.to_lowercase().as_str() {
        "debug" => SimpleLogger::init(LevelFilter::Debug, LogConfig::default())?,
        "info" => SimpleLogger::init(LevelFilter::Info, LogConfig::default())?,
        _ => SimpleLogger::init(LevelFilter::Error, LogConfig::default())?,
    }
    info!("{:?}", cfg.clone());
    info!("FuseQuery v-{}", cfg.version);

    let cluster = Cluster::create(cfg.clone());

    // MySQL handler.
    {
        let session_mgr = Session::create();
        let mysql_handler = MySQLHandler::create(cfg.clone(), session_mgr, cluster.clone());
        tokio::spawn(async move { mysql_handler.start() });

        info!(
            "MySQL handler listening on {}:{}, Usage: mysql -h{} -P{}",
            cfg.mysql_handler_host,
            cfg.mysql_handler_port,
            cfg.mysql_handler_host,
            cfg.mysql_handler_port
        );
    }

    // RPC server.
    {
        let rpc_addr = cfg.rpc_api_address.parse()?;
        info!("RPC Server listening on {}", rpc_addr);

        let rpc_executor = ExecutorRPCServer::default();
        Server::builder()
            .add_service(ExecutorServer::new(rpc_executor))
            .serve(rpc_addr)
            .await?;
    }

    // Admin API.
    {
        let admin = Admin::create(cfg.clone(), cluster);
        admin.start().await?;
    }

    // Metrics exporter.
    {
        let metric = Metric::create(cfg.clone());
        metric.start()?;
        info!(
            "Prometheus exporter listening on {}",
            cfg.prometheus_exporter_address
        );
    }

    // Wait.
    signal(SignalKind::hangup())?.recv().await;
    Ok(())
}
