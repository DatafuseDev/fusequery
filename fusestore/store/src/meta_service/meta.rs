// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::ClientRequest;
use crate::meta_service::ClientResponse;
use crate::meta_service::Cmd;
use crate::meta_service::IPlacement;
use crate::meta_service::NodeId;

/// Meta data of a Dfs.
/// Includes:
/// - what files are stored in this Dfs.
/// - the algo about how to distribute files to nodes.
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Meta {
    /// The file names stored in this cluster
    pub keys: BTreeMap<String, String>,

    // cluster nodes, key distribution etc.
    pub slots: Vec<Slot>,
    pub nodes: HashMap<NodeId, Node>,
}

impl Meta {
    // Apply an op from client.
    #[tracing::instrument(level = "info", skip(self))]
    pub fn apply(&mut self, data: &ClientRequest) -> anyhow::Result<ClientResponse> {
        match data.cmd {
            Cmd::AddFile { ref key, ref value } => {
                if self.keys.contains_key(key) {
                    let prev = self.keys.get(key);
                    Ok((prev.cloned(), None).into())
                } else {
                    let prev = self.keys.insert(key.clone(), value.clone());
                    tracing::info!("applied AddFile: {}={}", key, value);
                    Ok((prev, Some(value.clone())).into())
                }
            }

            Cmd::SetFile { ref key, ref value } => {
                let prev = self.keys.insert(key.clone(), value.clone());
                tracing::info!("applied SetFile: {}={}", key, value);
                Ok((prev, Some(value.clone())).into())
            }

            Cmd::AddNode {
                ref node_id,
                ref node,
            } => {
                if self.nodes.contains_key(node_id) {
                    let prev = self.nodes.get(node_id);
                    Ok((prev.cloned(), None).into())
                } else {
                    let prev = self.nodes.insert(*node_id, node.clone());
                    tracing::info!("applied AddNode: {}={:?}", node_id, node);
                    Ok((prev, Some(node.clone())).into())
                }
            }
        }
    }

    #[tracing::instrument(level = "info", skip(self))]
    pub fn get_file(&self, key: &str) -> Option<String> {
        tracing::info!("meta::get_file: {}", key);
        let x = self.keys.get(key);
        tracing::info!("meta::get_file: {}={:?}", key, x);
        x.cloned()
    }

    pub fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let x = self.nodes.get(node_id);
        x.cloned()
    }
}

/// A slot is a virtual and intermediate allocation unit in a distributed storage.
/// The key of an object is mapped to a slot by some hashing algo.
/// A slot is assigned to several physical servers(normally 3 for durability).
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct Slot {
    pub node_ids: Vec<NodeId>,
}

#[derive(Serialize, Deserialize, Debug, Default, Clone, PartialEq)]
pub struct Node {
    pub name: String,
    pub address: String,
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}={}", self.name, self.address)
    }
}

impl IPlacement for Meta {
    fn get_slots(&self) -> &[Slot] {
        &self.slots
    }

    fn get_node(&self, node_id: &u64) -> Option<&Node> {
        self.nodes.get(node_id)
    }
}
