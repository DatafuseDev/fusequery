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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfo;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_pipeline_transforms::processors::transforms::AccumulatingTransform;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::SegmentInfo;

use super::ConflictResolveContext;
use super::SnapshotChanges;
use crate::operations::common::AbortOperation;
use crate::operations::mutation::MutationDeletedSegment;
use crate::statistics::merge_statistics;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Default)]
pub struct MutationLogs {
    pub entries: Vec<MutationLogEntry>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum MutationLogEntry {
    AppendSegment {
        segment_location: String,
        segment_info: Arc<SegmentInfo>,
        format_version: FormatVersion,
    },
    DeletedBlock {
        index: BlockMetaIndex,
    },
    DeletedSegment {
        deleted_segment: MutationDeletedSegment,
    },
    Replaced {
        index: BlockMetaIndex,
        block_meta: Arc<BlockMeta>,
    },
    DoNothing,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq)]
pub struct BlockMetaIndex {
    pub segment_idx: usize,
    pub block_idx: usize,
    // range is unused for now.
    // pub range: Option<Range<usize>>,
}

#[typetag::serde(name = "mutation_logs_meta")]
impl BlockMetaInfo for MutationLogs {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        Self::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl From<MutationLogs> for DataBlock {
    fn from(value: MutationLogs) -> Self {
        let block_meta = Box::new(value);
        DataBlock::empty_with_meta(block_meta)
    }
}

impl TryFrom<DataBlock> for MutationLogs {
    type Error = ErrorCode;
    fn try_from(value: DataBlock) -> std::result::Result<Self, Self::Error> {
        let block_meta = value.get_owned_meta().ok_or_else(|| {
            ErrorCode::Internal(
                "converting data block meta to MutationLogs failed, no data block meta found",
            )
        })?;
        MutationLogs::downcast_from(block_meta).ok_or_else(|| {
            ErrorCode::Internal("downcast block meta to MutationLogs failed, type mismatch")
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct CommitMeta {
    pub conflict_resolve_context: ConflictResolveContext,
    pub abort_operation: AbortOperation,
}

impl CommitMeta {
    pub fn empty() -> Self {
        CommitMeta {
            conflict_resolve_context: ConflictResolveContext::ModifiedSegmentExistsInLatest(
                SnapshotChanges::default(),
            ),
            abort_operation: AbortOperation::default(),
        }
    }
}

fn merge_conflict_resolve_context(
    l: ConflictResolveContext,
    r: ConflictResolveContext,
    default_cluster_key_id: Option<u32>,
) -> ConflictResolveContext {
    match (l, r) {
        (
            ConflictResolveContext::ModifiedSegmentExistsInLatest(l),
            ConflictResolveContext::ModifiedSegmentExistsInLatest(r),
        ) => ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
            removed_segment_indexes: l
                .removed_segment_indexes
                .into_iter()
                .chain(r.removed_segment_indexes.into_iter())
                .collect(),
            added_segments: l
                .added_segments
                .into_iter()
                .chain(r.added_segments.into_iter())
                .collect(),
            removed_statistics: merge_statistics(
                &l.removed_statistics,
                &r.added_statistics,
                default_cluster_key_id,
            ),
            added_statistics: merge_statistics(
                &l.added_statistics,
                &r.added_statistics,
                default_cluster_key_id,
            ),
        }),
        _ => unreachable!(
            "conflict resolve context to be merged should both be ModifiedSegmentExistsInLatest"
        ),
    }
}

fn merge_commit_meta(
    l: CommitMeta,
    r: CommitMeta,
    default_cluster_key_id: Option<u32>,
) -> CommitMeta {
    CommitMeta {
        conflict_resolve_context: merge_conflict_resolve_context(
            l.conflict_resolve_context,
            r.conflict_resolve_context,
            default_cluster_key_id,
        ),
        abort_operation: AbortOperation {
            segments: l
                .abort_operation
                .segments
                .into_iter()
                .chain(r.abort_operation.segments.into_iter())
                .collect(),
            blocks: l
                .abort_operation
                .blocks
                .into_iter()
                .chain(r.abort_operation.blocks.into_iter())
                .collect(),
            bloom_filter_indexes: l
                .abort_operation
                .bloom_filter_indexes
                .into_iter()
                .chain(r.abort_operation.bloom_filter_indexes.into_iter())
                .collect(),
        },
    }
}

impl CommitMeta {
    pub fn new(
        conflict_resolve_context: ConflictResolveContext,
        abort_operation: AbortOperation,
    ) -> Self {
        CommitMeta {
            conflict_resolve_context,
            abort_operation,
        }
    }
}

#[typetag::serde(name = "commit_meta")]
impl BlockMetaInfo for CommitMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        Self::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl TryFrom<DataBlock> for CommitMeta {
    type Error = ErrorCode;
    fn try_from(value: DataBlock) -> std::result::Result<Self, Self::Error> {
        let block_meta = value.get_owned_meta().ok_or_else(|| {
            ErrorCode::Internal(
                "converting data block meta to CommitMeta failed, no data block meta found",
            )
        })?;
        CommitMeta::downcast_from(block_meta).ok_or_else(|| {
            ErrorCode::Internal("downcast block meta to CommitMeta failed, type mismatch")
        })
    }
}

impl From<CommitMeta> for DataBlock {
    fn from(value: CommitMeta) -> Self {
        let block_meta = Box::new(value);
        DataBlock::empty_with_meta(block_meta)
    }
}

pub struct TransformMergeCommitMeta {
    to_merged: Vec<CommitMeta>,
    default_cluster_key_id: Option<u32>,
}

impl TransformMergeCommitMeta {
    pub fn create(default_cluster_key_id: Option<u32>) -> Self {
        TransformMergeCommitMeta {
            to_merged: vec![],
            default_cluster_key_id,
        }
    }
}

impl AccumulatingTransform for TransformMergeCommitMeta {
    const NAME: &'static str = "TransformMergeCommitMeta";

    fn transform(
        &mut self,
        data: common_expression::DataBlock,
    ) -> common_exception::Result<Vec<common_expression::DataBlock>> {
        let commit_meta = CommitMeta::try_from(data)?;
        self.to_merged.push(commit_meta);
        Ok(vec![])
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        let to_merged = std::mem::take(&mut self.to_merged);
        let merged = to_merged.into_iter().fold(CommitMeta::empty(), |acc, x| {
            merge_commit_meta(acc, x, self.default_cluster_key_id)
        });
        Ok(vec![merged.into()])
    }
}
