// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use warp::Filter;

use crate::clusters::ClusterRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ClusterNodeRequest {
    pub name: String,
    pub cpus: usize,
    // Priority is in [0, 10]
    // Larger value means higher
    // priority
    pub priority: u8,
    pub address: String,
}

pub fn cluster_handler(
    cluster: ClusterRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    cluster_list_node(cluster.clone())
        .or(cluster_add_node(cluster.clone()))
        .or(cluster_remove_node(cluster))
}

/// GET /v1/cluster/list
fn cluster_list_node(
    cluster: ClusterRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "list")
        .and(warp::get())
        .and(with_cluster(cluster))
        .and_then(handlers::list_node)
}

fn cluster_add_node(
    cluster: ClusterRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "add")
        .and(warp::post())
        .and(json_body())
        .and(with_cluster(cluster))
        .and_then(handlers::add_node)
}

fn cluster_remove_node(
    cluster: ClusterRef,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("v1" / "cluster" / "remove")
        .and(warp::post())
        .and(json_body())
        .and(with_cluster(cluster))
        .and_then(handlers::remove_node)
}

fn with_cluster(
    cluster: ClusterRef,
) -> impl Filter<Extract = (ClusterRef,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || cluster.clone())
}

fn json_body() -> impl Filter<Extract = (ClusterNodeRequest,), Error = warp::Rejection> + Clone {
    // When accepting a body, we want a JSON body
    // (and to reject huge payloads)...
    warp::body::content_length_limit(1024 * 16).and(warp::body::json())
}

mod handlers {
    use log::info;

    use crate::clusters::{ClusterRef, Node};
    use crate::rpcs::http::v1::cluster::ClusterNodeRequest;

    pub async fn list_node(
        cluster: ClusterRef,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        // TODO(BohuTANG): error handler
        let nodes = cluster.get_nodes().unwrap();
        Ok(warp::reply::json(&nodes))
    }

    pub async fn add_node(
        req: ClusterNodeRequest,
        cluster: ClusterRef,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        info!("Cluster add node: {:?}", req);
        // TODO(BohuTANG): error handler
        cluster
            .add_node(&Node {
                name: req.name,
                cpus: req.cpus,
                priority: req.priority,
                address: req.address,
                local: false,
            })
            .unwrap();
        Ok(warp::http::StatusCode::OK)
    }

    pub async fn remove_node(
        req: ClusterNodeRequest,
        cluster: ClusterRef,
    ) -> Result<impl warp::Reply, std::convert::Infallible> {
        info!("Cluster remove node: {:?}", req);
        // TODO(BohuTANG): error handler
        cluster.remove_node(req.name).unwrap();
        Ok(warp::http::StatusCode::OK)
    }
}
