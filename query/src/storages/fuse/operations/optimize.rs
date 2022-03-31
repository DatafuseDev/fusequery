//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashSet;
use std::sync::Arc;

use common_cache::Cache;
use common_exception::ErrorCode;
use common_exception::Result;
use opendal::Operator;

use crate::sessions::QueryContext;
use crate::storages::fuse::io::MetaReaders;
use crate::storages::fuse::meta::Location;
use crate::storages::fuse::FuseTable;
use crate::storages::fuse::FUSE_OPT_KEY_SNAPSHOT_LOC;
use crate::storages::Table;

impl FuseTable {
    pub async fn do_optimize(
        &self,
        ctx: Arc<QueryContext>,
        keep_last_snapshot: bool,
    ) -> Result<()> {
        let accessor = ctx.get_storage_operator()?;
        let tbl_info = self.get_table_info();
        let snapshot_loc = tbl_info.meta.options.get(FUSE_OPT_KEY_SNAPSHOT_LOC);
        let format_version = self.snapshot_format_version();
        let reader = MetaReaders::table_snapshot_reader(ctx.as_ref());

        let mut snapshots = reader
            .read_snapshot_history(
                snapshot_loc,
                format_version,
                self.meta_location_generator.clone(),
            )
            .await?;

        let min_history_len = if !keep_last_snapshot { 0 } else { 1 };

        // short cut
        if snapshots.len() <= min_history_len {
            return Ok(());
        }

        let current_segments: HashSet<&Location>;
        let current_snapshot;
        if !keep_last_snapshot {
            // if truncate_all requested, gc root contains nothing;
            current_segments = HashSet::new();
        } else {
            current_snapshot = snapshots.remove(0);
            current_segments = HashSet::from_iter(&current_snapshot.segments);
        }

        let prevs = snapshots.iter().fold(HashSet::new(), |mut acc, s| {
            acc.extend(&s.segments);
            acc
        });

        // segments which no longer need to be kept
        let seg_delta = prevs.difference(&current_segments).collect::<Vec<_>>();

        // TODO rm those deference **
        // blocks to be removed
        let prev_blocks: HashSet<String> = self
            .blocks_of(seg_delta.iter().map(|i| **i), ctx.clone())
            .await?;
        let current_blocks: HashSet<String> = self
            .blocks_of(current_segments.iter().copied(), ctx.clone())
            .await?;
        let block_delta = prev_blocks.difference(&current_blocks);

        // NOTE: the following actions are NOT transactional yet

        // 1. remove blocks
        for x in block_delta {
            self.remove_location(accessor.clone(), x).await?;
        }

        // 2. remove the segments
        for (x, _v) in seg_delta {
            self.remove_location(accessor.clone(), x.as_str()).await?;
            if let Some(c) = ctx.get_storage_cache_manager().get_table_segment_cache() {
                let cache = &mut *c.write().await;
                cache.pop(x.as_str());
            }
        }

        let locs = self.meta_location_generator();
        // 3. remove the snapshots
        for s in snapshots.iter().rev() {
            let loc = locs.snapshot_location_from_uuid(&s.snapshot_id, s.format_version())?;
            self.remove_location(accessor.clone(), loc.as_str()).await?;
            if let Some(c) = ctx.get_storage_cache_manager().get_table_snapshot_cache() {
                let cache = &mut *c.write().await;
                cache.pop(loc.as_str());
            }
        }

        Ok(())
    }

    async fn blocks_of(
        &self,
        //locations: impl Iterator<Item = impl AsRef<Location>>,
        locations: impl Iterator<Item = &Location>,
        ctx: Arc<QueryContext>,
    ) -> Result<HashSet<String>> {
        let mut result = HashSet::new();
        let reader = MetaReaders::segment_info_reader(ctx.as_ref());
        for l in locations {
            //let (x, ver) = l.as_ref();
            let (x, ver) = l;
            let res = reader.read(x, None, *ver).await?;
            for block_meta in &res.blocks {
                result.insert(block_meta.location.0.clone());
            }
        }
        Ok(result)
    }

    async fn remove_location(
        &self,
        data_accessor: Operator,
        location: impl AsRef<str>,
    ) -> Result<()> {
        data_accessor
            .object(location.as_ref())
            .delete()
            .await
            .map_err(|e| ErrorCode::DalTransportError(e.to_string()))
    }
}
