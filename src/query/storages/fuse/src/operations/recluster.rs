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
use std::sync::Arc;

use common_catalog::plan::DataSourceInfo;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PruningStatistics;
use common_catalog::plan::PushDownInfo;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_transforms::processors::transforms::build_merge_sort_pipeline;
use common_pipeline_transforms::processors::transforms::AsyncAccumulatingTransformer;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::MutationKind;
use log::info;
use storages_common_table_meta::meta::BlockMeta;

use crate::operations::common::BlockMetaIndex;
use crate::operations::common::CommitSink;
use crate::operations::common::MutationGenerator;
use crate::operations::common::TableMutationAggregator;
use crate::operations::common::TransformSerializeBlock;
use crate::operations::common::TransformSerializeSegment;
use crate::operations::mutation::MAX_BLOCK_COUNT;
use crate::operations::ReclusterMutator;
use crate::pipelines::Pipeline;
use crate::pruning::create_segment_location_vector;
use crate::pruning::FusePruner;
use crate::FuseTable;
use crate::DEFAULT_AVG_DEPTH_THRESHOLD;
use crate::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;

impl FuseTable {
    /// The flow of Pipeline is as follows:
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┐
    // └──────────┘     └───────────────┘     └─────────┘    │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │     ┌──────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┤────►│MultiSortMerge├────►│Resize(N)├───┐
    // └──────────┘     └───────────────┘     └─────────┘    │     └──────────────┘     └─────────┘   │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │                                        │
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┘                                        │
    // └──────────┘     └───────────────┘     └─────────┘                                             │
    // ┌──────────────────────────────────────────────────────────────────────────────────────────────┘
    // │         ┌──────────────┐
    // │    ┌───►│SerializeBlock├───┐
    // │    │    └──────────────┘   │
    // │    │    ┌──────────────┐   │    ┌─────────┐    ┌────────────────┐     ┌─────────────────┐     ┌──────────┐
    // └───►│───►│SerializeBlock├───┤───►│Resize(1)├───►│SerializeSegment├────►│TableMutationAggr├────►│CommitSink│
    //      │    └──────────────┘   │    └─────────┘    └────────────────┘     └─────────────────┘     └──────────┘
    //      │    ┌──────────────┐   │
    //      └───►│SerializeBlock├───┘
    //           └──────────────┘
    #[async_backtrace::framed]
    pub(crate) async fn do_recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        limit: Option<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<u64> {
        if self.cluster_key_meta.is_none() {
            return Ok(0);
        }

        let snapshot_opt = self.read_table_snapshot().await?;
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no recluster.
            return Ok(0);
        };

        let default_cluster_key_id = self.cluster_key_meta.clone().unwrap().0;
        let block_thresholds = self.get_block_thresholds();
        let avg_depth_threshold = self.get_option(
            FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD,
            DEFAULT_AVG_DEPTH_THRESHOLD,
        );
        let block_count = snapshot.summary.block_count;
        let threshold = (block_count as f64 * avg_depth_threshold).max(1.0);
        let mut mutator = ReclusterMutator::try_create(ctx.clone(), threshold, block_thresholds)?;

        let schema = self.table_info.schema();
        let segment_locations = snapshot.segments.clone();
        let segment_locations = create_segment_location_vector(segment_locations, None);
        let mut pruner = FusePruner::create(
            &ctx,
            self.operator.clone(),
            schema,
            &push_downs,
            self.bloom_index_cols(),
        )?;

        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let limit = limit.unwrap_or(max_threads * 4);
        for chunk in segment_locations.chunks(limit) {
            let mut block_metas = pruner.read_pruning(chunk.to_vec()).await?;
            block_metas.truncate(MAX_BLOCK_COUNT);

            let mut blocks_map: BTreeMap<i32, Vec<(BlockMetaIndex, Arc<BlockMeta>)>> =
                BTreeMap::new();
            block_metas.into_iter().for_each(|(idx, b)| {
                if let Some(stats) = &b.cluster_stats {
                    if stats.cluster_key_id == default_cluster_key_id && stats.level >= 0 {
                        blocks_map.entry(stats.level).or_default().push((
                            BlockMetaIndex {
                                segment_idx: idx.segment_idx,
                                block_idx: idx.block_idx,
                            },
                            b,
                        ));
                    }
                }
            });

            if mutator.target_select(blocks_map).await? {
                break;
            }
        }

        let block_metas: Vec<_> = mutator
            .take_blocks()
            .iter()
            .map(|meta| (None, meta.clone()))
            .collect();
        let block_count = block_metas.len();
        if block_count < 2 {
            return Ok(0);
        }

        // Status.
        {
            let status = format!(
                "recluster: select block files: {}, total bytes: {}, total rows: {}",
                block_count, mutator.total_bytes, mutator.total_rows,
            );
            ctx.set_status_info(&status);
            info!("{}", status);
        }

        let (statistics, parts) = self.read_partitions_with_metas(
            self.table_info.schema(),
            None,
            &block_metas,
            None,
            block_count,
            PruningStatistics::default(),
        )?;
        let table_info = self.get_table_info();
        let description = statistics.get_description(&table_info.desc);
        let plan = DataSourcePlan {
            catalog: table_info.catalog().to_string(),
            source_info: DataSourceInfo::TableSource(table_info.clone()),
            output_schema: table_info.schema(),
            parts,
            statistics,
            description,
            tbl_args: self.table_args(),
            push_downs: None,
            query_internal_columns: false,
            data_mask_policy: None,
        };

        ctx.set_partitions(plan.parts.clone())?;

        // ReadDataKind to avoid OOM.
        self.do_read_data(ctx.clone(), &plan, pipeline)?;

        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), mutator.level + 1, block_thresholds)?;
        let operators = cluster_stats_gen.operators.clone();
        if !operators.is_empty() {
            let num_input_columns = self.table_info.schema().fields().len();
            let func_ctx2 = cluster_stats_gen.func_ctx.clone();
            pipeline.add_transform(move |input, output| {
                Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    func_ctx2.clone(),
                    operators.clone(),
                )))
            })?;
        }

        // merge sort
        let block_num = mutator
            .total_bytes
            .div_ceil(block_thresholds.max_bytes_per_block);
        let final_block_size = std::cmp::min(
            // estimate block_size based on max_bytes_per_block.
            mutator.total_rows / block_num,
            block_thresholds.max_rows_per_block,
        );
        let partial_block_size = if pipeline.output_len() > 1 {
            std::cmp::min(
                final_block_size,
                ctx.get_settings().get_max_block_size()? as usize,
            )
        } else {
            final_block_size
        };
        // construct output fields
        let output_fields: Vec<DataField> = cluster_stats_gen.out_fields.clone();
        let schema = DataSchemaRefExt::create(output_fields);
        let sort_descs: Vec<SortColumnDescription> = cluster_stats_gen
            .cluster_key_index
            .iter()
            .map(|offset| SortColumnDescription {
                offset: *offset,
                asc: true,
                nulls_first: false,
                is_nullable: false, // This information is not needed here.
            })
            .collect();

        build_merge_sort_pipeline(
            pipeline,
            schema,
            sort_descs,
            None,
            partial_block_size,
            final_block_size,
            None,
        )?;

        let output_block_num = mutator.total_rows.div_ceil(final_block_size);
        let max_threads = std::cmp::min(max_threads, output_block_num);
        pipeline.try_resize(max_threads)?;
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            let proc = TransformSerializeBlock::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                self,
                cluster_stats_gen.clone(),
            )?;
            proc.into_processor()
        })?;

        pipeline.try_resize(1)?;
        pipeline.add_transform(|input, output| {
            let proc = TransformSerializeSegment::new(input, output, self, block_thresholds);
            proc.into_processor()
        })?;

        pipeline.add_transform(|input, output| {
            let mut aggregator = TableMutationAggregator::create(
                self,
                ctx.clone(),
                snapshot.segments.clone(),
                snapshot.summary.clone(),
                MutationKind::Recluster,
            );
            aggregator.accumulate_log_entry(mutator.mutation_logs());
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input, output, aggregator,
            )))
        })?;

        let snapshot_gen = MutationGenerator::new(snapshot);
        pipeline.add_sink(|input| {
            CommitSink::try_create(
                self,
                ctx.clone(),
                None,
                snapshot_gen.clone(),
                input,
                None,
                true,
                None,
            )
        })?;
        Ok(block_count as u64)
    }
}
