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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use common_base::runtime::execute_futures_in_parallel;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::TableSchemaRef;
use opendal::Operator;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::Versioned;
use tracing::info;

use super::ConflictResolveContext;
use super::SnapshotChanges;
use super::SnapshotMerged;
use crate::io::SegmentsIO;
use crate::io::SerializedSegment;
use crate::io::TableMetaLocationGenerator;
use crate::operations::common::AbortOperation;
use crate::operations::common::CommitMeta;
use crate::operations::common::MutationLogEntry;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::MutationDeletedSegment;
use crate::operations::mutation::SegmentIndex;
use crate::statistics::reducers::deduct_statistics_mut;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;
use crate::FuseTable;

#[derive(Default)]
struct BlockMutations {
    replaced_blocks: Vec<(BlockIndex, Arc<BlockMeta>)>,
    deleted_blocks: Vec<BlockIndex>,
}

impl BlockMutations {
    fn new_replacement(block_idx: BlockIndex, block_meta: Arc<BlockMeta>) -> Self {
        BlockMutations {
            replaced_blocks: vec![(block_idx, block_meta)],
            deleted_blocks: vec![],
        }
    }

    fn new_deletion(block_idx: BlockIndex) -> Self {
        BlockMutations {
            replaced_blocks: vec![],
            deleted_blocks: vec![block_idx],
        }
    }

    fn push_replaced(&mut self, block_idx: BlockIndex, block_meta: Arc<BlockMeta>) {
        self.replaced_blocks.push((block_idx, block_meta));
    }

    fn push_deleted(&mut self, block_idx: BlockIndex) {
        self.deleted_blocks.push(block_idx)
    }
}

#[derive(Clone, Copy)]
/// This is used by MutationAccumulator, so no compact here.
pub enum MutationKind {
    Delete,
    Update,
    Replace,
    Recluster,
    Insert,
}

pub struct MutationAccumulator {
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,
    thresholds: BlockThresholds,
    default_cluster_key_id: Option<u32>,

    mutations: HashMap<SegmentIndex, BlockMutations>,
    deleted_segments: Vec<MutationDeletedSegment>,
    // (path, segment_info)
    appended_segments: Vec<(String, Arc<SegmentInfo>, FormatVersion)>,
    base_segments: Vec<Location>,

    abort_operation: AbortOperation,
    summary: Statistics,
    kind: MutationKind,
}

impl MutationAccumulator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        base_segments: Vec<Location>,
        summary: Statistics,
        kind: MutationKind,
    ) -> Self {
        MutationAccumulator {
            ctx,
            schema: table.schema(),
            dal: table.get_operator(),
            location_gen: table.meta_location_generator().clone(),
            thresholds: table.get_block_thresholds(),
            default_cluster_key_id: table.cluster_key_id(),
            mutations: HashMap::new(),
            appended_segments: vec![],
            base_segments,
            abort_operation: AbortOperation::default(),
            summary,
            deleted_segments: vec![],
            kind,
        }
    }

    pub fn accumulate_log_entry(&mut self, log_entry: MutationLogEntry) {
        match log_entry {
            MutationLogEntry::Replaced { index, block_meta } => {
                self.mutations
                    .entry(index.segment_idx)
                    .and_modify(|v| v.push_replaced(index.block_idx, block_meta.clone()))
                    .or_insert(BlockMutations::new_replacement(
                        index.block_idx,
                        block_meta.clone(),
                    ));
                self.abort_operation.add_block(&block_meta);
            }
            MutationLogEntry::DeletedBlock { index } => {
                self.mutations
                    .entry(index.segment_idx)
                    .and_modify(|v| v.push_deleted(index.block_idx))
                    .or_insert(BlockMutations::new_deletion(index.block_idx));
            }
            MutationLogEntry::DeletedSegment { deleted_segment } => {
                self.deleted_segments.push(deleted_segment)
            }
            MutationLogEntry::DoNothing => (),
            MutationLogEntry::AppendSegment {
                segment_location,
                segment_info,
                format_version,
            } => {
                for block_meta in &segment_info.blocks {
                    self.abort_operation.add_block(block_meta);
                }
                // TODO can we avoid this clone?
                self.abort_operation.add_segment(segment_location.clone());

                self.appended_segments
                    .push((segment_location, segment_info, format_version))
            }
        }
    }
}

impl MutationAccumulator {
    pub async fn apply(&mut self) -> Result<CommitMeta> {
        let start = Instant::now();
        let mut count = 0;

        let chunk_size = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        let segment_indices = self.mutations.keys().cloned().collect::<Vec<_>>();
        let mut removed_segment_indexes = Vec::with_capacity(segment_indices.len());
        let mut added_segments = vec![];
        let mut removed_statistics = Statistics::default();
        let mut added_statistics = Statistics::default();
        for s in &self.deleted_segments {
            removed_segment_indexes.push(s.deleted_segment.index);
            added_segments.push(None);
            merge_statistics_mut(
                &mut removed_statistics,
                &s.deleted_segment.segment_info.1,
                self.default_cluster_key_id,
            );
        }
        for chunk in segment_indices.chunks(chunk_size) {
            let results = self.partial_apply(chunk.to_vec()).await?;
            for result in results {
                if let Some((location, summary)) = result.new_segment_info {
                    // replace the old segment location with the new one.
                    self.abort_operation.add_segment(location.clone());
                    merge_statistics_mut(
                        &mut added_statistics,
                        &summary,
                        self.default_cluster_key_id,
                    );
                    added_segments.push(Some((location, SegmentInfo::VERSION)));
                } else {
                    added_segments.push(None);
                }
                removed_segment_indexes.push(result.index);
                merge_statistics_mut(
                    &mut removed_statistics,
                    &result.origin_summary,
                    self.default_cluster_key_id,
                );
            }

            // Refresh status
            {
                count += chunk.len();
                let status = format!(
                    "mutation: generate new segment files:{}/{}, cost:{} sec",
                    count,
                    segment_indices.len(),
                    start.elapsed().as_secs()
                );
                self.ctx.set_status_info(&status);
            }
        }

        for (_path, new_segment, _format_version) in &self.appended_segments {
            merge_statistics_mut(
                &mut self.summary,
                &new_segment.summary,
                self.default_cluster_key_id,
            );
        }

        let conflict_resolve_context = match self.kind {
            MutationKind::Delete => {
                info!("removed_segment_indexes:{:?}", removed_segment_indexes);
                ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
                    removed_segment_indexes,
                    added_segments,
                    removed_statistics,
                    added_statistics,
                })
            }
            _ => {
                merge_statistics_mut(
                    &mut self.summary,
                    &added_statistics,
                    self.default_cluster_key_id,
                );
                deduct_statistics_mut(&mut self.summary, &removed_statistics);
                let merged_segments = ConflictResolveContext::merge_segments(
                    std::mem::take(&mut self.base_segments),
                    added_segments,
                    removed_segment_indexes,
                );
                let merged_segments = self
                    .appended_segments
                    .iter()
                    .map(|(path, _segment, format_version)| (path.clone(), *format_version))
                    .chain(merged_segments)
                    .collect();
                match self.kind {
                    MutationKind::Insert => ConflictResolveContext::AppendOnly((
                        SnapshotMerged {
                            merged_segments,
                            merged_statistics: self.summary.clone(),
                        },
                        self.schema.clone(),
                    )),
                    _ => ConflictResolveContext::LatestSnapshotAppendOnly(SnapshotMerged {
                        merged_segments,
                        merged_statistics: self.summary.clone(),
                    }),
                }
            }
        };

        let meta = CommitMeta::new(conflict_resolve_context, self.abort_operation.clone());
        Ok(meta)
    }

    async fn partial_apply(&mut self, segment_indices: Vec<usize>) -> Result<Vec<SegmentLite>> {
        let thresholds = self.thresholds;
        let mut tasks = Vec::with_capacity(segment_indices.len());
        for index in segment_indices {
            let segment_mutation = self.mutations.remove(&index).unwrap();
            let location = self.base_segments[index].clone();
            let schema = self.schema.clone();
            let op = self.dal.clone();
            let location_gen = self.location_gen.clone();
            let default_cluster_key_id = self.default_cluster_key_id;

            tasks.push(async move {
                // read the old segment
                let mut segment_info =
                    SegmentsIO::read_segment(op.clone(), location, schema, false).await?;

                // take away the blocks, they are being mutated
                let mut block_editor = BTreeMap::<_, _>::from_iter(
                    std::mem::take(&mut segment_info.blocks)
                        .into_iter()
                        .enumerate(),
                );
                for (idx, new_meta) in segment_mutation.replaced_blocks {
                    block_editor.insert(idx, new_meta);
                }
                for idx in segment_mutation.deleted_blocks {
                    block_editor.remove(&idx);
                }

                if !block_editor.is_empty() {
                    // assign back the mutated blocks to segment
                    let new_blocks = block_editor.into_values().collect::<Vec<_>>();
                    // re-calculate the segment statistics
                    let new_summary =
                        reduce_block_metas(&new_blocks, thresholds, default_cluster_key_id);
                    // create new segment info
                    let new_segment = SegmentInfo::new(new_blocks, new_summary.clone());

                    // write the segment info.
                    let location = location_gen.gen_segment_info_location();
                    let serialized_segment = SerializedSegment {
                        path: location.clone(),
                        segment: Arc::new(new_segment),
                    };
                    SegmentsIO::write_segment(op, serialized_segment).await?;

                    Ok(SegmentLite {
                        index,
                        new_segment_info: Some((location, new_summary)),
                        origin_summary: segment_info.summary,
                    })
                } else {
                    Ok(SegmentLite {
                        index,
                        new_segment_info: None,
                        origin_summary: segment_info.summary,
                    })
                }
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let permit_nums = self.ctx.get_settings().get_max_storage_io_requests()? as usize;
        execute_futures_in_parallel(
            tasks,
            threads_nums,
            permit_nums,
            "fuse-req-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()
    }
}

struct SegmentLite {
    // segment index.
    index: usize,
    // new segment location and summary.
    new_segment_info: Option<(String, Statistics)>,
    // origin segment summary.
    origin_summary: Statistics,
}
