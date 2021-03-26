// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::configs::Config;
use crate::error::ClusterResult;
use crate::Node;

pub type ClusterRef = Arc<Cluster>;

pub struct Cluster {
    cfg: Config,
    nodes: Mutex<HashMap<String, Node>>,
}

impl Cluster {
    pub fn create(cfg: Config) -> ClusterRef {
        Arc::new(Cluster {
            cfg,
            nodes: Mutex::new(HashMap::new()),
        })
    }

    pub fn empty() -> ClusterRef {
        Arc::new(Cluster {
            cfg: Config::default(),
            nodes: Mutex::new(HashMap::new()),
        })
    }

    pub fn is_empty(&self) -> ClusterResult<bool> {
        Ok(self.nodes.lock()?.len() == 0)
    }

    pub fn add_node(&self, n: &Node) -> ClusterResult<()> {
        let mut node = Node {
            name: n.name.clone(),
            cpus: n.cpus,
            // To set priority when adding node to the cluster
            // We need to add a key-value in the json input,
            // such as {... , "priority":10, ...}.
            // The value of "priority" must be in [0,10].
            priority: n.priority,
            address: n.address.clone(),
            local: false,
        };
        if node.address == self.cfg.rpc_api_address {
            node.local = true;
        }
        self.nodes.lock()?.insert(node.name.clone(), node);
        Ok(())
    }

    pub fn remove_node(&self, id: String) -> ClusterResult<()> {
        self.nodes.lock()?.remove(&*id);
        Ok(())
    }

    pub fn get_nodes(&self) -> ClusterResult<Vec<Node>> {
        let mut nodes = vec![];

        for (_, node) in self.nodes.lock()?.iter() {
            nodes.push(node.clone());
        }
        nodes.sort_by(|a, b| b.name.cmp(&a.name));
        Ok(nodes)
    }
}
