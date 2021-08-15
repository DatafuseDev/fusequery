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

use std::fmt::Debug;
use std::fmt::Formatter;

use common_exception::ErrorCode;
use warp::reject::Reject;
use warp::Filter;

use crate::clusters::ClusterRef;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct ClusterNodeRequest {
    pub name: String,
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

    use crate::api::http::v1::cluster::ClusterNodeRequest;
    use crate::api::http::v1::cluster::NoBacktraceErrorCode;
    use crate::clusters::ClusterRef;

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
    ) -> Result<impl warp::Reply, warp::Rejection> {
        info!("Cluster add node: {:?}", req);
        match cluster
            .add_node(&req.name, req.priority, &req.address)
            .await
        {
            Ok(_) => Ok(warp::reply::with_status(
                "".to_string(),
                warp::http::StatusCode::OK,
            )),
            Err(error_codes) => Err(warp::reject::custom(NoBacktraceErrorCode(error_codes))),
        }
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

struct NoBacktraceErrorCode(ErrorCode);

impl Debug for NoBacktraceErrorCode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Reject for NoBacktraceErrorCode {}
