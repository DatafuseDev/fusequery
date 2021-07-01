use std::fmt;

use async_raft::NodeId;
use serde::Deserialize;
use serde::Serialize;

use crate::meta_service::Node;

// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.
/// Cmd is an action a client wants to take.
/// A Cmd is committed by raft leader before being applied.

/// A Cmd describes what a user want to do to raft state machine
/// and is the essential part of a raft log.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum Cmd {
    /// AKA put-if-absent. add a key-value record only when key is absent.
    AddFile {
        key: String,
        value: String,
    },

    /// Override the record with key.
    SetFile {
        key: String,
        value: String,
    },

    /// Increment the sequence number generator specified by `key` and returns the new value.
    IncrSeq {
        key: String,
    },

    /// Add node if absent
    AddNode {
        node_id: NodeId,
        node: Node,
    },

    /// Add a database if absent
    AddDatabase {
        name: String,
    },

    /// Update or insert a general purpose kv store
    UpsertKV {
        key: String,
        /// Set to Some() to modify the value only when the seq matches.
        /// Since a sequence number is positive, use Some(0) to perform an add-if-absent operation.
        seq: Option<u64>,
        value: Vec<u8>,
    },
    DeleteByKeyKV {
        key: String,
        seq: Option<u64>,
    },

    UpdateByKeyKV {
        key: String,
        seq: Option<u64>,
        value: Vec<u8>,
    },
}

impl fmt::Display for Cmd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Cmd::AddFile { key, value } => {
                write!(f, "add_file:{}={}", key, value)
            }
            Cmd::SetFile { key, value } => {
                write!(f, "set_file:{}={}", key, value)
            }
            Cmd::IncrSeq { key } => {
                write!(f, "incr_seq:{}", key)
            }
            Cmd::AddNode { node_id, node } => {
                write!(f, "add_node:{}={}", node_id, node)
            }
            Cmd::AddDatabase { name } => {
                write!(f, "add_db:{}", name)
            }
            Cmd::UpsertKV { key, seq, value } => {
                write!(f, "upsert_kv: {}({:?}) = {:?}", key, seq, value)
            }
            Cmd::DeleteByKeyKV { key, seq } => {
                write!(f, "delete_by_key_kv: {}({:?})", key, seq)
            }
            Cmd::UpdateByKeyKV { key, seq, value } => {
                write!(f, "update_kv: {}({:?}) = {:?}", key, seq, value)
            }
        }
    }
}
