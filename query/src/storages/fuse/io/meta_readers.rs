// Copyright 2021 Datafuse Labs.
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

use std::io::ErrorKind;
use std::sync::Arc;

use common_arrow::arrow::io::parquet::read::read_metadata_async;
use common_arrow::arrow::io::parquet::read::schema::FileMetaData;
use common_dal::InputStream;
use common_exception::ErrorCode;
use common_exception::Result;
use common_tracing::tracing::debug_span;
use common_tracing::tracing::Instrument;
use serde::de::DeserializeOwned;

use crate::sessions::QueryContext;
use crate::storages::fuse::cache::CachedReader;
use crate::storages::fuse::cache::HasTenantLabel;
use crate::storages::fuse::cache::Loader;
use crate::storages::fuse::cache::MemoryCache;
use crate::storages::fuse::cache::TenantLabel;
use crate::storages::fuse::io::snapshot_location;
use crate::storages::fuse::meta::SegmentInfo;
use crate::storages::fuse::meta::TableSnapshot;

/// Provider of [InputStream]
///
/// Mainly used as a auxiliary facility in the implementation of [Loader], such that the acquirement
/// of an [InputStream] can be deferred or avoided (e.g. if hits cache).
pub trait InputStreamProvider {
    fn input_stream(&self, path: &str, len: Option<u64>) -> Result<InputStream>;
}

/// A Newtype for [FileMetaData].
///
/// To avoid implementation (of trait [Loader]) conflicts
pub struct BlockMeta(FileMetaData);

impl BlockMeta {
    pub fn inner(&self) -> &FileMetaData {
        &self.0
    }
}

pub type SegmentInfoCache = MemoryCache<SegmentInfo>;
pub type TableSnapshotCache = MemoryCache<TableSnapshot>;
pub type BlockMetaCache = MemoryCache<BlockMeta>;

pub type SegmentInfoReader<'a> = CachedReader<SegmentInfo, &'a QueryContext>;
pub type TableSnapshotReader<'a> = CachedReader<TableSnapshot, &'a QueryContext>;

/// A sugar type of BlockMeta reader
///
/// To make it "lifetime-compliant", `Arc<QueryContext>` is used as the `Loader` of [CachedReader],
/// instead of `&QueryContext`.  (BlockMetaReader is used in constructing async streams)
///
pub type BlockMetaReader = CachedReader<BlockMeta, Arc<QueryContext>>;

/// Aux struct, factory of common readers
pub struct MetaReaders;

impl MetaReaders {
    pub fn segment_info_reader(ctx: &QueryContext) -> SegmentInfoReader {
        SegmentInfoReader::new(
            ctx.get_storage_cache_manager().get_table_segment_cache(),
            ctx,
            "SEGMENT_INFO_CACHE".to_owned(),
        )
    }

    pub fn table_snapshot_reader(ctx: &QueryContext) -> TableSnapshotReader {
        TableSnapshotReader::new(
            ctx.get_storage_cache_manager().get_table_snapshot_cache(),
            ctx,
            "SNAPSHOT_CACHE".to_owned(),
        )
    }

    pub fn block_meta_reader(ctx: Arc<QueryContext>) -> BlockMetaReader {
        BlockMetaReader::new(
            ctx.get_storage_cache_manager().get_block_meta_cache(),
            ctx,
            "BLOCK_META_CACHE".to_owned(),
        )
    }
}

impl<'a> TableSnapshotReader<'a> {
    pub async fn read_snapshot_history(
        &self,
        latest_snapshot_location: Option<&String>,
    ) -> Result<Vec<Arc<TableSnapshot>>> {
        let mut snapshots = vec![];
        let mut current_snapshot_location = latest_snapshot_location.cloned();
        while let Some(loc) = current_snapshot_location {
            let r = self.read(loc).await;
            let snapshot = match r {
                Ok(s) => s,
                Err(e) if e.code() == ErrorCode::dal_path_not_found_code() => {
                    break;
                }
                Err(e) => return Err(e),
            };
            let prev = snapshot.prev_snapshot_id;
            snapshots.push(snapshot);
            current_snapshot_location = prev.map(|id| snapshot_location(&id));
        }
        Ok(snapshots)
    }
}

#[async_trait::async_trait]
impl<T, V> Loader<V> for T
where
    T: InputStreamProvider + Sync,
    V: DeserializeOwned,
{
    async fn load(&self, key: &str, length_hint: Option<u64>) -> Result<V> {
        let mut reader = self.input_stream(key, length_hint)?;
        let mut buffer = vec![];

        use futures::AsyncReadExt;
        reader.read_to_end(&mut buffer).await.map_err(|e| {
            let msg = e.to_string();
            if e.kind() == ErrorKind::NotFound {
                ErrorCode::DalPathNotFound(msg)
            } else {
                ErrorCode::DalTransportError(msg)
            }
        })?;
        let r = serde_json::from_slice::<V>(&buffer)?;
        Ok(r)
    }
}

#[async_trait::async_trait]
impl<T> Loader<BlockMeta> for T
where T: InputStreamProvider + Sync
{
    async fn load(&self, key: &str, length_hint: Option<u64>) -> Result<BlockMeta> {
        let mut reader = self.input_stream(key, length_hint)?;
        let meta = read_metadata_async(&mut reader)
            .instrument(debug_span!("parquet_source_read_meta"))
            .await
            .map_err(|e| ErrorCode::ParquetError(e.to_string()))?;
        Ok(BlockMeta(meta))
    }
}

impl InputStreamProvider for &QueryContext {
    fn input_stream(&self, path: &str, len: Option<u64>) -> Result<InputStream> {
        let accessor = self.get_storage_accessor()?;
        accessor.get_input_stream(path, len)
    }
}

impl InputStreamProvider for Arc<QueryContext> {
    fn input_stream(&self, path: &str, len: Option<u64>) -> Result<InputStream> {
        self.as_ref().input_stream(path, len)
    }
}

impl HasTenantLabel for &QueryContext {
    fn tenant_label(&self) -> TenantLabel {
        ctx_tenant_label(self)
    }
}

impl HasTenantLabel for Arc<QueryContext> {
    fn tenant_label(&self) -> TenantLabel {
        ctx_tenant_label(self)
    }
}

fn ctx_tenant_label(ctx: &QueryContext) -> TenantLabel {
    let mgr = ctx.get_storage_cache_manager();
    TenantLabel {
        tenant_id: mgr.get_tenant_id(),
        cluster_id: mgr.get_cluster_id(),
    }
}
