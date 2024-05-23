// Copyright 2021 Datafuse Labs
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

use std::io;
use std::io::ErrorKind;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyerror::AnyError;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::io::AsyncBufReadExt;
use databend_common_base::base::tokio::io::BufReader;
use databend_common_base::base::tokio::sync::RwLock;
use databend_common_base::base::tokio::sync::RwLockWriteGuard;
use databend_common_meta_raft_store::config::RaftConfig;
use databend_common_meta_raft_store::key_spaces::RaftStateKV;
use databend_common_meta_raft_store::key_spaces::RaftStoreEntry;
use databend_common_meta_raft_store::leveled_store::sys_data_api::SysDataApiRO;
use databend_common_meta_raft_store::log::RaftLog;
use databend_common_meta_raft_store::ondisk::DATA_VERSION;
use databend_common_meta_raft_store::ondisk::TREE_HEADER;
use databend_common_meta_raft_store::sm_v002::SnapshotStoreError;
use databend_common_meta_raft_store::sm_v002::SnapshotStoreV002;
use databend_common_meta_raft_store::sm_v002::SnapshotViewV002;
use databend_common_meta_raft_store::sm_v002::WriteEntry;
use databend_common_meta_raft_store::sm_v002::SMV002;
use databend_common_meta_raft_store::state::RaftState;
use databend_common_meta_raft_store::state::RaftStateKey;
use databend_common_meta_raft_store::state::RaftStateValue;
use databend_common_meta_raft_store::state_machine::MetaSnapshotId;
use databend_common_meta_sled_store::get_sled_db;
use databend_common_meta_sled_store::SledTree;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::Endpoint;
use databend_common_meta_types::LogId;
use databend_common_meta_types::Membership;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use databend_common_meta_types::MetaStartupError;
use databend_common_meta_types::Node;
use databend_common_meta_types::NodeId;
use databend_common_meta_types::Snapshot;
use databend_common_meta_types::SnapshotData;
use databend_common_meta_types::SnapshotMeta;
use databend_common_meta_types::StorageError;
use databend_common_meta_types::StorageIOError;
use databend_common_meta_types::Vote;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::info;
use log::warn;

use crate::export::vec_kv_to_json;
use crate::Opened;

/// This is the inner store that provides support utilities for implementing the raft storage API.
///
/// This store is backed by a sled db, contents are stored in 3 trees:
///   state:
///       id
///       vote
///   log
///   state_machine
pub struct StoreInner {
    /// The ID of the Raft node for which this storage instances is configured.
    /// ID is also stored in raft_state. Since `id` never changes, this is a cache for fast access.
    pub id: NodeId,

    pub(crate) config: RaftConfig,

    /// If the instance is opened from an existent state(e.g. load from fs) or created.
    is_opened: bool,

    /// The sled db for log, raft_state and state machine.
    pub(crate) db: sled::Db,

    /// Raft state includes:
    /// id: NodeId,
    ///     vote,      // the last `Vote`
    ///     committed, // last `LogId` that is known committed
    pub raft_state: RwLock<RaftState>,

    /// A series of raft logs.
    pub log: RwLock<RaftLog>,

    /// The Raft state machine.
    pub state_machine: Arc<RwLock<SMV002>>,

    /// The current snapshot.
    pub current_snapshot: RwLock<Option<SnapshotMeta>>,
}

impl AsRef<StoreInner> for StoreInner {
    fn as_ref(&self) -> &StoreInner {
        self
    }
}

impl Opened for StoreInner {
    /// If the instance is opened(true) from an existent state(e.g. load from fs) or created(false).
    fn is_opened(&self) -> bool {
        self.is_opened
    }
}

impl StoreInner {
    /// Open an existent `metasrv` instance or create an new one:
    /// 1. If `open` is `Some`, try to open an existent one.
    /// 2. If `create` is `Some`, try to create one.
    /// Otherwise it panic
    #[minitrace::trace]
    pub async fn open_create(
        config: &RaftConfig,
        open: Option<()>,
        create: Option<()>,
    ) -> Result<StoreInner, MetaStartupError> {
        info!(config_id :% =(&config.config_id); "open: {:?}, create: {:?}", open, create);

        let db = get_sled_db();

        let raft_state = RaftState::open_create(&db, config, open, create).await?;
        let is_open = raft_state.is_open();
        info!("RaftState opened is_open: {}", is_open);

        let log = RaftLog::open(&db, config).await?;
        info!("RaftLog opened");

        fn to_startup_err(e: impl std::error::Error + 'static) -> MetaStartupError {
            let ae = AnyError::new(&e);
            let store_err = MetaStorageError::SnapshotError(ae);
            MetaStartupError::StoreOpenError(store_err)
        }

        let snapshot_store = SnapshotStoreV002::new(DATA_VERSION, config.clone());
        let last = snapshot_store
            .load_last_snapshot()
            .await
            .map_err(to_startup_err)?;

        let (sm, snapshot_meta) = if let Some((id, snapshot)) = last {
            let (sm, meta) = Self::rebuild_state_machine(&id, snapshot)
                .await
                .map_err(to_startup_err)?;

            info!(
                "rebuilt state machine from last snapshot({:?}), meta: {:?}",
                id, meta
            );

            if id.key_num.is_none() {
                warn!(
                    "no key num embedded in snapshot id: {:?}; key num will be updated next time building/installing snapshot",
                    id
                );
            }

            (sm, Some(meta))
        } else {
            info!("No snapshot, skip rebuilding state machine");
            (Default::default(), None)
        };

        let store = Self {
            id: raft_state.id,
            config: config.clone(),
            is_opened: is_open,
            db,
            raft_state: RwLock::new(raft_state),
            log: RwLock::new(log),
            state_machine: sm,
            current_snapshot: RwLock::new(None),
        };

        store.set_snapshot(snapshot_meta).await;

        Ok(store)
    }

    /// Return a snapshot store of this instance.
    pub fn snapshot_store(&self) -> SnapshotStoreV002 {
        SnapshotStoreV002::new(DATA_VERSION, self.config.clone())
    }

    async fn rebuild_state_machine(
        id: &MetaSnapshotId,
        snapshot: SnapshotData,
    ) -> Result<(Arc<RwLock<SMV002>>, SnapshotMeta), io::Error> {
        info!("rebuild state machine from last snapshot({:?})", id);

        let sm = Arc::new(RwLock::new(SMV002::default()));

        SMV002::install_snapshot(sm.clone(), Box::new(snapshot)).await?;

        let (last_applied, last_membership) = {
            let sm = sm.read().await;
            let last_applied = *sm.sys_data_ref().last_applied_ref();
            let last_membership = sm.sys_data_ref().last_membership_ref().clone();

            (last_applied, last_membership)
        };

        let meta = SnapshotMeta {
            snapshot_id: id.to_string(),
            last_log_id: last_applied,
            last_membership,
        };

        Ok((sm, meta))
    }

    /// Get a handle to the state machine for testing purposes.
    pub async fn get_state_machine(&self) -> RwLockWriteGuard<'_, SMV002> {
        self.state_machine.write().await
    }

    #[minitrace::trace]
    pub(crate) async fn do_build_snapshot(&self) -> Result<Snapshot, StorageError> {
        // NOTE: building snapshot is guaranteed to be serialized called by RaftCore.

        info!(id = self.id; "do_build_snapshot start");

        let snapshot_view = self
            .build_compacted_snapshot()
            .await
            .map_err(|e| StorageIOError::read_snapshot(None, &e))?;

        let mut snapshot_meta = snapshot_view.build_snapshot_meta();
        let meta = snapshot_meta.clone();
        let signature = snapshot_meta.signature();

        info!("do_build_snapshot writing snapshot start");

        let mut strm = snapshot_view.export().await.map_err(|e| {
            SnapshotStoreError::read(e).with_meta("export state machine", &snapshot_meta)
        })?;

        let context = format!("build snapshot: {}", meta.snapshot_id);
        let ss_store = self.snapshot_store();
        let writer = ss_store.new_writer()?;
        let (tx, th) = writer.spawn_writer_thread(context);

        // Pipe entries to the writer.
        {
            while let Some(ent) = strm
                .try_next()
                .await
                .map_err(|e| StorageIOError::read_snapshot(Some(signature.clone()), &e))?
            {
                tx.send(WriteEntry::Data(ent))
                    .await
                    .map_err(|e| StorageIOError::write_snapshot(Some(signature.clone()), &e))?;
            }
            tx.send(WriteEntry::Finish)
                .await
                .map_err(|e| StorageIOError::write_snapshot(Some(signature.clone()), &e))?;
        }

        // Get snapshot write result
        let (temp_snapshot_data, snapshot_stat) = th
            .await
            .map_err(|e| {
                error!(error :% = e; "snapshot writer thread error");
                StorageIOError::write_snapshot(Some(signature.clone()), &e)
            })?
            .map_err(|e| {
                error!(error :% = e; "snapshot writer thread error");
                StorageIOError::write_snapshot(Some(signature.clone()), &e)
            })?;

        let (snapshot_id, snapshot_data) = ss_store
            .commit_snapshot_data_gen_id(
                temp_snapshot_data,
                snapshot_meta.last_log_id,
                snapshot_stat.entry_cnt,
            )
            .map_err(|e| {
                error!(error :% = e; "commit temp snapshot error");
                StorageIOError::write_snapshot(Some(signature.clone()), &e)
            })?;

        info!(
            snapshot_id :% = snapshot_id.to_string(),
            snapshot_stat :% = snapshot_stat; "do_build_snapshot complete");

        // Clean old snapshot
        ss_store.clean_old_snapshots().await?;
        info!("do_build_snapshot clean_old_snapshots complete");

        snapshot_meta.snapshot_id = snapshot_id.to_string();

        self.set_snapshot(Some(snapshot_meta.clone())).await;

        Ok(Snapshot {
            meta: snapshot_meta,
            snapshot: Box::new(snapshot_data),
        })
    }

    /// Return the delay config for testing.
    async fn get_delay_config(&self, key: &str) -> Duration {
        if cfg!(debug_assertions) {
            let sm = self.get_state_machine().await;
            let c = sm.blocking_config();
            match key {
                "write" => c.write_snapshot,
                "compact" => c.compact_snapshot,
                _ => {
                    unreachable!("unknown key: {}", key);
                }
            }
        } else {
            Duration::from_secs(0)
        }
    }

    /// Sleep in blocking mode for testing.
    fn testing_sleep(key: &str, sleep: Duration) {
        #[allow(clippy::collapsible_if)]
        if cfg!(debug_assertions) {
            if !sleep.is_zero() {
                warn!("start    {} sleep: {:?}", key, sleep);
                std::thread::sleep(sleep);
                warn!("finished {} sleep", key);
            }
        }
    }

    /// Store the last snapshot in memory.
    ///
    /// The snapshot is a small object and the snapshot data is store on disk.
    pub(crate) async fn set_snapshot(&self, snapshot_meta: Option<SnapshotMeta>) {
        info!("set_snapshot: {:?}", snapshot_meta);
        let mut current_snapshot = self.current_snapshot.write().await;
        *current_snapshot = snapshot_meta;
    }

    /// Return snapshot id and meta of the last snapshot.
    ///
    /// It returns None if there is no snapshot or there is an error parsing snapshot meta or id.
    pub(crate) async fn try_get_snapshot_info(&self) -> Option<(MetaSnapshotId, SnapshotMeta)> {
        let meta = {
            let current_snapshot = self.current_snapshot.read().await;
            current_snapshot.clone()
        };

        let meta = meta?;

        let Ok(id) = MetaSnapshotId::from_str(&meta.snapshot_id) else {
            warn!("invalid snapshot id: {:?}", meta);
            return None;
        };

        Some((id, meta))
    }

    /// Build and compact a snapshot view.
    ///
    /// - Take a snapshot view of the current state machine;
    /// - Compact multi levels in the snapshot view into one to get rid of tombstones;
    async fn build_compacted_snapshot(&self) -> Result<SnapshotViewV002, io::Error> {
        let mut snapshot_view = {
            let mut s = self.state_machine.write().await;
            s.full_snapshot_view()
        };

        // Compact multi levels into one to get rid of tombstones
        // Move heavy load task to a blocking thread pool.
        tokio::task::block_in_place({
            let s = &mut snapshot_view;
            let sleep = self.get_delay_config("compact").await;

            move || {
                Self::testing_sleep("compact", sleep);
                // TODO: this is a future never returning Pending:
                futures::executor::block_on(s.compact_mem_levels())
            }
        })?;

        // State machine ensures no modification to `base` during snapshotting.
        {
            let mut s = self.state_machine.write().await;
            s.replace_frozen(&snapshot_view);
        }

        Ok(snapshot_view)
    }

    /// Install a snapshot to build a state machine from it and replace the old state machine with the new one.
    #[minitrace::trace]
    pub async fn do_install_snapshot(
        &self,
        data: Box<SnapshotData>,
    ) -> Result<(), MetaStorageError> {
        SMV002::install_snapshot(self.state_machine.clone(), data)
            .await
            .map_err(|e| {
                MetaStorageError::SnapshotError(
                    AnyError::new(&e).add_context(|| "replacing state-machine with snapshot"),
                )
            })?;

        // TODO(xp): use checksum to check consistency?

        Ok(())
    }

    /// Export data that can be used to restore a meta-service node.
    ///
    /// Returns a `BoxStream<'a, Result<String, io::Error>>` that yields a series of JSON strings.
    #[futures_async_stream::try_stream(boxed, ok = String, error = io::Error)]
    pub async fn export(self: Arc<StoreInner>) {
        // Convert an error occurred during export to `io::Error(InvalidData)`.
        fn invalid_data(e: impl std::error::Error + Send + Sync + 'static) -> io::Error {
            io::Error::new(ErrorKind::InvalidData, e)
        }

        // Lock all data components so that we have a consistent view.
        //
        // Hold the snapshot lock to prevent snapshot from being replaced until exporting finished.
        // Holding this lock prevent logs from being purged.
        //
        // Although vote and log must be consistent,
        // it is OK to export RaftState and logs without transaction protection(i.e. they do not share a lock),
        // if it guarantees no logs have a greater `vote` than `RaftState.HardState`.
        let current_snapshot = self.current_snapshot.read().await;
        let raft_state = self.raft_state.read().await;
        let log = self.log.read().await;

        // Export data header first
        {
            let header_tree = SledTree::open(&self.db, TREE_HEADER, false).map_err(invalid_data)?;

            let header_kvs = header_tree.export()?;

            for kv in header_kvs.iter() {
                let line = vec_kv_to_json(TREE_HEADER, kv)?;
                yield line;
            }
        }

        // Export raft state
        {
            let tree_name = &raft_state.inner.name;

            let ks = raft_state.inner.key_space::<RaftStateKV>();

            let id = ks.get(&RaftStateKey::Id)?.map(NodeId::from);

            if let Some(id) = id {
                let ent_id = RaftStoreEntry::RaftStateKV {
                    key: RaftStateKey::Id,
                    value: RaftStateValue::NodeId(id),
                };

                let s = serde_json::to_string(&(tree_name, ent_id)).map_err(invalid_data)?;
                yield s;
            }

            let vote = ks.get(&RaftStateKey::HardState)?.map(Vote::from);

            if let Some(vote) = vote {
                let ent_vote = RaftStoreEntry::RaftStateKV {
                    key: RaftStateKey::HardState,
                    value: RaftStateValue::HardState(vote),
                };

                let s = serde_json::to_string(&(tree_name, ent_vote)).map_err(invalid_data)?;
                yield s;
            }

            let committed = ks
                .get(&RaftStateKey::Committed)?
                .and_then(Option::<LogId>::from);

            let ent_committed = RaftStoreEntry::RaftStateKV {
                key: RaftStateKey::Committed,
                value: RaftStateValue::Committed(committed),
            };

            let s = serde_json::to_string(&(tree_name, ent_committed)).map_err(invalid_data)?;
            yield s;
        };

        drop(raft_state);

        // Dump logs that has smaller or equal leader id as `vote`
        let log_tree_name = log.inner.name.clone();
        let log_kvs = log.inner.export()?;

        drop(log);

        // Dump snapshot of state machine

        // NOTE:
        // The name in form of "state_machine/[0-9]+" had been used by the sled tree based sm.
        // Do not change it for keeping compatibility.
        let sm_tree_name = "state_machine/0";
        let f = {
            let snapshot_meta = current_snapshot.clone();
            if let Some(meta) = snapshot_meta {
                let snapshot_store = SnapshotStoreV002::new(DATA_VERSION, self.config.clone());

                let f = snapshot_store
                    .load_snapshot(&meta.snapshot_id)
                    .await
                    .map_err(invalid_data)?;
                Some(f)
            } else {
                None
            }
        };

        drop(current_snapshot);

        for kv in log_kvs.iter() {
            let kv_entry = RaftStoreEntry::deserialize(&kv[0], &kv[1])?;

            let tree_kv = (&log_tree_name, kv_entry);
            let line = serde_json::to_string(&tree_kv).map_err(invalid_data)?;
            yield line;
        }

        if let Some(f) = f {
            let bf = BufReader::new(f);
            let mut lines = AsyncBufReadExt::lines(bf);

            while let Some(l) = lines.next_line().await? {
                let ent: RaftStoreEntry = serde_json::from_str(&l).map_err(invalid_data)?;

                let named_entry = (sm_tree_name, ent);

                let line = serde_json::to_string(&named_entry).map_err(invalid_data)?;
                yield line;
            }
        }
    }

    pub async fn get_node(&self, node_id: &NodeId) -> Option<Node> {
        let sm = self.state_machine.read().await;
        let n = sm.sys_data_ref().nodes_ref().get(node_id).cloned();
        n
    }

    /// Return a list of nodes of the corresponding node-ids returned by `list_ids`.
    pub(crate) async fn get_nodes(
        &self,
        list_ids: impl Fn(&Membership) -> Vec<NodeId>,
    ) -> Vec<Node> {
        let sm = self.state_machine.read().await;
        let membership = sm.sys_data_ref().last_membership_ref().membership();

        debug!("in-statemachine membership: {:?}", membership);

        let ids = list_ids(membership);
        debug!("filtered node ids: {:?}", ids);
        let mut ns = vec![];

        for id in ids {
            let node = sm.sys_data_ref().nodes_ref().get(&id).cloned();
            if let Some(x) = node {
                ns.push(x);
            }
        }

        ns
    }

    pub async fn get_node_raft_endpoint(&self, node_id: &NodeId) -> Result<Endpoint, MetaError> {
        let endpoint = self
            .get_node(node_id)
            .await
            .map(|n| n.endpoint)
            .ok_or_else(|| {
                MetaNetworkError::GetNodeAddrError(format!(
                    "fail to get endpoint of node_id: {}",
                    node_id
                ))
            })?;

        Ok(endpoint)
    }
}
