// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use async_raft::storage::HardState;
use common_exception::ErrorCode;
use common_tracing::tracing;

use crate::configs;
use crate::meta_service::AsKeySpace;
use crate::meta_service::NodeId;
use crate::meta_service::RaftStateKV;
use crate::meta_service::RaftStateKey;
use crate::meta_service::RaftStateValue;
use crate::meta_service::SledSerde;
use crate::meta_service::SledTree;

/// Raft state stores everything else other than log and state machine, which includes:
/// id: NodeId,
/// hard_state:
///      current_term,
///      voted_for,
///
#[derive(Debug)]
pub struct RaftState {
    pub id: NodeId,

    /// If the instance is opened(true) from an existent state(e.g. load from disk) or created(false).
    is_open: bool,

    /// A sled tree with key space support.
    pub(crate) inner: SledTree,
}

const TREE_RAFT_STATE: &str = "raft_state";

impl SledSerde for HardState {}

impl RaftState {
    pub fn is_open(&self) -> bool {
        self.is_open
    }
}

impl RaftState {
    /// Open/create a raft state in a sled db.
    /// 1. If `open` is `Some`,  it tries to open an existent RaftState if there is one.
    /// 2. If `create` is `Some`, it tries to initialize a new RaftState if there is not one.
    /// If none of them is `Some`, it is a programming error and will panic.
    #[tracing::instrument(level = "info", skip(db))]
    pub async fn open_create(
        db: &sled::Db,
        config: &configs::Config,
        open: Option<()>,
        create: Option<()>,
    ) -> common_exception::Result<RaftState> {
        let tree_name = config.tree_name(TREE_RAFT_STATE);
        let inner = SledTree::open(db, &tree_name, config.meta_sync()).await?;

        let state = inner.key_space::<RaftStateKV>();
        let curr_id = state.get(&RaftStateKey::Id)?.map(NodeId::from);

        tracing::debug!("get curr_id: {:?}", curr_id);

        let (id, is_open) = if let Some(curr_id) = curr_id {
            match (open, create) {
                (Some(_), _) => (curr_id, true),
                (None, Some(_)) => {
                    return Err(ErrorCode::MetaStoreAlreadyExists(format!(
                        "raft state present id={}, can not create",
                        curr_id
                    )));
                }
                (None, None) => panic!("no open no create"),
            }
        } else {
            match (open, create) {
                (Some(_), Some(_)) => (config.id, false),
                (Some(_), None) => {
                    return Err(ErrorCode::MetaStoreNotFound(
                        "raft state absent, can not open",
                    ));
                }
                (None, Some(_)) => (config.id, false),
                (None, None) => panic!("no open no create"),
            }
        };

        let rs = RaftState { id, is_open, inner };

        if !rs.is_open() {
            rs.init().await?;
        }

        Ok(rs)
    }

    /// Initialize a raft state. The only thing to do is to persist the node id
    /// so that next time opening it the caller knows it is initialized.
    #[tracing::instrument(level = "info", skip(self))]
    async fn init(&self) -> common_exception::Result<()> {
        let state = self.state();
        state
            .insert(&RaftStateKey::Id, &RaftStateValue::NodeId(self.id))
            .await?;
        Ok(())
    }

    pub async fn write_hard_state(&self, hs: &HardState) -> common_exception::Result<()> {
        let state = self.state();
        state
            .insert(
                &RaftStateKey::HardState,
                &RaftStateValue::HardState(hs.clone()),
            )
            .await?;
        Ok(())
    }

    pub fn read_hard_state(&self) -> common_exception::Result<Option<HardState>> {
        let state = self.state();
        let hs = state.get(&RaftStateKey::HardState)?;
        let hs = hs.map(HardState::from);
        Ok(hs)
    }

    pub async fn write_state_machine_id(&self, id: &(u64, u64)) -> common_exception::Result<()> {
        let state = self.state();
        state
            .insert(
                &RaftStateKey::StateMachineId,
                &RaftStateValue::StateMachineId(*id),
            )
            .await?;
        Ok(())
    }
    pub fn read_state_machine_id(&self) -> common_exception::Result<(u64, u64)> {
        let state = self.state();
        let smid = state.get(&RaftStateKey::StateMachineId)?;
        let smid: (u64, u64) = match smid {
            Some(v) => v.into(),
            None => (0, 0),
        };
        Ok(smid)
    }

    /// Returns a borrowed sled tree key space to store meta of raft log
    pub fn state(&self) -> AsKeySpace<RaftStateKV> {
        self.inner.key_space()
    }
}
