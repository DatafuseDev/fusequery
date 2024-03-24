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

use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;

use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::build_fuse_parquet_source_pipeline;
use crate::operations::read::fuse_source::build_fuse_native_source_pipeline;
use crate::pruning::InvertedIndexPruner;
use crate::pruning::SegmentLocation;
use crate::FuseSegmentPartInfo;
use crate::FuseStorageFormat;
use crate::FuseTable;

impl FuseTable {
    pub fn create_block_reader(
        &self,
        ctx: Arc<dyn TableContext>,
        projection: Projection,
        query_internal_columns: bool,
        update_stream_columns: bool,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        let table_schema = self.schema_with_stream();
        BlockReader::create(
            ctx,
            self.operator.clone(),
            table_schema,
            projection,
            query_internal_columns,
            update_stream_columns,
            put_cache,
        )
    }

    // Build the block reader.
    pub fn build_block_reader(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        put_cache: bool,
    ) -> Result<Arc<BlockReader>> {
        self.create_block_reader(
            ctx,
            PushDownInfo::projection_of_push_downs(
                &self.schema_with_stream(),
                plan.push_downs.as_ref(),
            ),
            plan.query_internal_columns,
            plan.update_stream_columns,
            put_cache,
        )
    }

    fn adjust_io_request(&self, ctx: &Arc<dyn TableContext>) -> Result<usize> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

        if !self.operator.info().native_capability().blocking {
            Ok(std::cmp::max(max_threads, max_io_requests))
        } else {
            // For blocking fs, we don't want this to be too large
            Ok(std::cmp::min(max_threads, max_io_requests).clamp(1, 48))
        }
    }

    fn apply_data_mask_policy_if_needed(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        if let Some(data_mask_policy) = &plan.data_mask_policy {
            let num_input_columns = plan.schema().num_fields();
            let mut exprs = Vec::with_capacity(num_input_columns);
            let mut projection = Vec::with_capacity(num_input_columns);
            let mut mask_count = 0;
            for i in 0..num_input_columns {
                if let Some(raw_expr) = data_mask_policy.get(&i) {
                    let expr = raw_expr.as_expr(&BUILTIN_FUNCTIONS);
                    exprs.push(expr.project_column_ref(|_col_id| i));
                    projection.push(mask_count + num_input_columns);
                    mask_count += 1;
                } else {
                    projection.push(i);
                }
            }

            let ops = vec![
                BlockOperator::Map {
                    exprs,
                    projections: None,
                },
                BlockOperator::Project { projection },
            ];

            let query_ctx = ctx.clone();
            let func_ctx = query_ctx.get_function_context()?;

            pipeline.add_transform(|input, output| {
                let transform = CompoundBlockOperator::create(
                    input,
                    output,
                    num_input_columns,
                    func_ctx.clone(),
                    ops.clone(),
                );
                Ok(ProcessorPtr::create(transform))
            })?;
        }

        Ok(())
    }

    #[inline]
    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        put_cache: bool,
    ) -> Result<()> {
        let snapshot_loc = plan.statistics.snapshot.clone();
        let index_info_locations = plan.statistics.index_info_locations.clone();
        let mut lazy_init_segments = Vec::with_capacity(plan.parts.len());

        for part in &plan.parts.partitions {
            if let Some(lazy_part_info) = part.as_any().downcast_ref::<FuseSegmentPartInfo>() {
                lazy_init_segments.push(SegmentLocation {
                    segment_idx: lazy_part_info.segment_index,
                    location: lazy_part_info.segment_location.clone(),
                    snapshot_loc: snapshot_loc.clone(),
                });
            }
        }
        if !lazy_init_segments.is_empty() {
            let table = self.clone();
            let table_schema = self.schema_with_stream();
            let push_downs = plan.push_downs.clone();
            let query_ctx = ctx.clone();
            let dal = self.operator.clone();

            // TODO: need refactor
            pipeline.set_on_init(move || {
                let is_inverted = push_downs
                    .as_ref()
                    .map(|p| p.inverted_index.is_some())
                    .unwrap_or_default();
                // First create an inverted index pruner, so that it can be
                // shared among partitions to avoid duplicate index initialize.
                let inverted_index_pruner = if is_inverted {
                    let dal = dal.clone();
                    let push_downs = push_downs.clone();
                    Runtime::with_default_worker_threads()?.block_on(async move {
                        let inverted_index_pruner = InvertedIndexPruner::try_create(
                            dal,
                            &push_downs,
                            &index_info_locations,
                        )
                        .await?;

                        Result::<_, ErrorCode>::Ok(inverted_index_pruner)
                    })?
                } else {
                    None
                };

                let table = table.clone();
                let table_schema = table_schema.clone();
                let ctx = query_ctx.clone();
                let dal = dal.clone();
                let push_downs = push_downs.clone();
                // let lazy_init_segments = lazy_init_segments.clone();

                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    let (_statistics, partitions) = table
                        .prune_snapshot_blocks(
                            ctx,
                            dal,
                            push_downs,
                            table_schema,
                            lazy_init_segments,
                            0,
                            inverted_index_pruner,
                        )
                        .await?;

                    Result::<_, ErrorCode>::Ok(partitions)
                })?;

                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        }

        let block_reader = self.build_block_reader(ctx.clone(), plan, put_cache)?;
        let max_io_requests = self.adjust_io_request(&ctx)?;

        let topk = plan
            .push_downs
            .as_ref()
            .filter(|_| self.is_native()) // Only native format supports topk push down.
            .and_then(|x| x.top_k(plan.schema().as_ref()));

        let index_reader = Arc::new(
            plan.push_downs
                .as_ref()
                .and_then(|p| p.agg_index.as_ref())
                .map(|agg| {
                    AggIndexReader::try_create(
                        ctx.clone(),
                        self.operator.clone(),
                        agg,
                        self.table_compression,
                        put_cache,
                    )
                })
                .transpose()?,
        );

        let virtual_reader = Arc::new(
            PushDownInfo::virtual_columns_of_push_downs(&plan.push_downs)
                .as_ref()
                .map(|virtual_columns| {
                    VirtualColumnReader::try_create(
                        ctx.clone(),
                        self.operator.clone(),
                        block_reader.schema(),
                        plan,
                        virtual_columns.clone(),
                        self.table_compression,
                        put_cache,
                    )
                })
                .transpose()?,
        );

        self.build_fuse_source_pipeline(
            ctx.clone(),
            pipeline,
            self.storage_format,
            block_reader,
            plan,
            topk,
            max_io_requests,
            index_reader,
            virtual_reader,
        )?;

        // replace the column which has data mask if needed
        self.apply_data_mask_policy_if_needed(ctx, plan, pipeline)?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn build_fuse_source_pipeline(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        plan: &DataSourcePlan,
        top_k: Option<TopK>,
        max_io_requests: usize,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<()> {
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let table_schema = self.schema_with_stream();
        match storage_format {
            FuseStorageFormat::Native => build_fuse_native_source_pipeline(
                ctx,
                table_schema,
                pipeline,
                block_reader,
                max_threads,
                plan,
                top_k,
                max_io_requests,
                index_reader,
                virtual_reader,
            ),
            FuseStorageFormat::Parquet => build_fuse_parquet_source_pipeline(
                ctx,
                table_schema,
                pipeline,
                block_reader,
                plan,
                max_threads,
                max_io_requests,
                index_reader,
                virtual_reader,
            ),
        }
    }
}
