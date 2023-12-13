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

use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::SortColumnDescription;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::query_spill_prefix;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::try_add_multi_sort_merge;
use common_pipeline_transforms::processors::ProcessorProfileWrapper;
use common_pipeline_transforms::processors::TransformSortMergeBuilder;
use common_pipeline_transforms::processors::TransformSortPartial;
use common_profile::SharedProcessorProfiles;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::physical_plans::Sort;
use common_storage::DataOperator;
use common_storages_fuse::TableContext;

use crate::pipelines::processors::transforms::create_transform_sort_spill;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

impl PipelineBuilder {
    // The pipeline graph of distributed sort can be found in https://github.com/datafuselabs/databend/pull/13881
    pub(crate) fn build_sort(&mut self, sort: &Sort) -> Result<()> {
        self.build_pipeline(&sort.input)?;

        let input_schema = sort.input.output_schema()?;

        if !matches!(sort.after_exchange, Some(true)) {
            // If the Sort plan is after exchange, we don't need to do a projection,
            // because the data is already projected in each cluster node.
            if let Some(proj) = &sort.pre_projection {
                // Do projection to reduce useless data copying during sorting.
                let projection = proj
                    .iter()
                    .filter_map(|i| input_schema.index_of(&i.to_string()).ok())
                    .collect::<Vec<_>>();

                if projection.len() < input_schema.fields().len() {
                    // Only if the projection is not a full projection, we need to add a projection transform.
                    self.main_pipeline.add_transform(|input, output| {
                        Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                            input,
                            output,
                            input_schema.num_fields(),
                            self.func_ctx.clone(),
                            vec![BlockOperator::Project {
                                projection: projection.clone(),
                            }],
                        )))
                    })?;
                }
            }
        }

        let input_schema = sort.output_schema()?;

        let sort_desc = sort
            .order_by
            .iter()
            .map(|desc| {
                let offset = input_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                    is_nullable: input_schema.field(offset).is_nullable(),  // This information is not needed here.
                })
            })
            .collect::<Result<Vec<_>>>()?;

        self.build_sort_pipeline(
            input_schema,
            sort_desc,
            sort.plan_id,
            sort.limit,
            sort.after_exchange,
        )
    }

    pub(crate) fn build_sort_pipeline(
        &mut self,
        input_schema: DataSchemaRef,
        sort_desc: Vec<SortColumnDescription>,
        plan_id: u32,
        limit: Option<usize>,
        after_exchange: Option<bool>,
    ) -> Result<()> {
        let block_size = self.settings.get_max_block_size()? as usize;
        let max_threads = self.settings.get_max_threads()? as usize;
        let sort_desc = Arc::new(sort_desc);

        // TODO(Winter): the query will hang in MultiSortMergeProcessor when max_threads == 1 and output_len != 1
        if self.main_pipeline.output_len() == 1 || max_threads == 1 {
            self.main_pipeline.try_resize(max_threads)?;
        }
        let prof_info = if self.enable_profiling {
            Some((plan_id, self.proc_profs.clone()))
        } else {
            None
        };

        let mut builder =
            SortPipelineBuilder::create(self.ctx.clone(), input_schema.clone(), sort_desc.clone())
                .with_partial_block_size(block_size)
                .with_final_block_size(block_size)
                .with_limit(limit)
                .with_prof_info(prof_info.clone());

        match after_exchange {
            Some(true) => {
                // Build for the coordinator node.
                // We only build a `MultiSortMergeTransform`,
                // as the data is already sorted in each cluster node.
                // The input number of the transform is equal to the number of cluster nodes.
                if self.main_pipeline.output_len() > 1 {
                    try_add_multi_sort_merge(
                        &mut self.main_pipeline,
                        input_schema,
                        block_size,
                        limit,
                        sort_desc,
                        prof_info,
                        true,
                    )
                } else {
                    builder = builder.remove_order_col_at_last();
                    builder.build_merge_sort_pipeline(&mut self.main_pipeline, true)
                }
            }
            Some(false) => {
                // Build for each cluster node.
                // We build the full sort pipeline for it.
                // Don't remove the order column at last.
                builder.build_full_sort_pipeline(&mut self.main_pipeline)
            }
            None => {
                // Build for single node mode.
                // We build the full sort pipeline for it.
                builder = builder.remove_order_col_at_last();
                builder.build_full_sort_pipeline(&mut self.main_pipeline)
            }
        }
    }
}

pub struct SortPipelineBuilder {
    ctx: Arc<QueryContext>,
    schema: DataSchemaRef,
    sort_desc: Arc<Vec<SortColumnDescription>>,
    limit: Option<usize>,
    partial_block_size: usize,
    final_block_size: usize,
    prof_info: Option<(u32, SharedProcessorProfiles)>,
    remove_order_col_at_last: bool,
}

impl SortPipelineBuilder {
    pub fn create(
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        sort_desc: Arc<Vec<SortColumnDescription>>,
    ) -> Self {
        Self {
            ctx,
            schema,
            sort_desc,
            limit: None,
            partial_block_size: 0,
            final_block_size: 0,
            prof_info: None,
            remove_order_col_at_last: false,
        }
    }

    pub fn with_limit(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_partial_block_size(mut self, partial_block_size: usize) -> Self {
        self.partial_block_size = partial_block_size;
        self
    }

    pub fn with_final_block_size(mut self, final_block_size: usize) -> Self {
        self.final_block_size = final_block_size;
        self
    }

    pub fn with_prof_info(mut self, prof_info: Option<(u32, SharedProcessorProfiles)>) -> Self {
        self.prof_info = prof_info;
        self
    }

    pub fn remove_order_col_at_last(mut self) -> Self {
        self.remove_order_col_at_last = true;
        self
    }

    pub fn build_full_sort_pipeline(self, pipeline: &mut Pipeline) -> Result<()> {
        // Partial sort
        pipeline.add_transform(|input, output| {
            let transform = TransformSortPartial::try_create(
                input,
                output,
                self.limit,
                self.sort_desc.clone(),
            )?;
            if let Some((plan_id, prof)) = &self.prof_info {
                Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                    transform,
                    *plan_id,
                    prof.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        self.build_merge_sort_pipeline(pipeline, false)
    }

    fn get_memory_settings(&self, num_threads: usize) -> Result<(usize, usize)> {
        let settings = self.ctx.get_settings();
        let memory_ratio = settings.get_sort_spilling_memory_ratio()?;
        let bytes_limit_per_proc = settings.get_sort_spilling_bytes_threshold_per_proc()?;
        if memory_ratio == 0 && bytes_limit_per_proc == 0 {
            // If these two settings are not set, do not enable sort spill.
            // TODO(spill): enable sort spill by default like aggregate.
            return Ok((0, 0));
        }
        let memory_ratio = (memory_ratio as f64 / 100_f64).min(1_f64);
        let max_memory_usage = match settings.get_max_memory_usage()? {
            0 => usize::MAX,
            max_memory_usage => {
                if memory_ratio == 0_f64 {
                    usize::MAX
                } else {
                    (max_memory_usage as f64 * memory_ratio) as usize
                }
            }
        };
        let spill_threshold_per_core =
            match settings.get_sort_spilling_bytes_threshold_per_proc()? {
                0 => max_memory_usage / num_threads,
                bytes => bytes,
            };

        Ok((max_memory_usage, spill_threshold_per_core))
    }

    pub fn build_merge_sort_pipeline(
        self,
        pipeline: &mut Pipeline,
        order_col_generated: bool,
    ) -> Result<()> {
        // Merge sort
        let need_multi_merge = pipeline.output_len() > 1;
        let output_order_col = need_multi_merge || !self.remove_order_col_at_last;
        debug_assert!(if order_col_generated {
            // If `order_col_generated`, it means this transform is the last processor in the distributed sort pipeline.
            !output_order_col
        } else {
            true
        });

        let (max_memory_usage, bytes_limit_per_proc) =
            self.get_memory_settings(pipeline.output_len())?;

        let may_spill = max_memory_usage != 0 && bytes_limit_per_proc != 0;

        pipeline.add_transform(|input, output| {
            let builder = TransformSortMergeBuilder::create(
                input,
                output,
                self.schema.clone(),
                self.sort_desc.clone(),
                self.partial_block_size,
            )
            .with_limit(self.limit)
            .with_order_col_generated(order_col_generated)
            .with_output_order_col(output_order_col || may_spill)
            .with_max_memory_usage(max_memory_usage)
            .with_spilling_bytes_threshold_per_core(bytes_limit_per_proc);

            let transform = builder.build()?;
            if let Some((plan_id, prof)) = &self.prof_info {
                Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                    transform,
                    *plan_id,
                    prof.clone(),
                )))
            } else {
                Ok(ProcessorPtr::create(transform))
            }
        })?;

        if may_spill {
            let config = SpillerConfig::create(query_spill_prefix(&self.ctx.get_tenant()));
            // The input of the processor must contain an order column.
            let schema = if let Some(f) = self.schema.fields.last() && f.name() == "_order_col" {
                self.schema.clone()
            } else {
                let mut fields = self.schema.fields().clone();
                fields.push(DataField::new(
                    "_order_col",
                    order_column_type(&self.sort_desc, &self.schema),
                ));
                DataSchemaRefExt::create(fields)
            };
            pipeline.add_transform(|input, output| {
                let op = DataOperator::instance().operator();
                let spiller =
                    Spiller::create(self.ctx.clone(), op, config.clone(), SpillerType::OrderBy);
                let transform = create_transform_sort_spill(
                    input,
                    output,
                    schema.clone(),
                    self.sort_desc.clone(),
                    spiller,
                    output_order_col,
                );
                if let Some((plan_id, prof)) = &self.prof_info {
                    Ok(ProcessorPtr::create(ProcessorProfileWrapper::create(
                        transform,
                        *plan_id,
                        prof.clone(),
                    )))
                } else {
                    Ok(ProcessorPtr::create(transform))
                }
            })?;
        }

        if need_multi_merge {
            // Multi-pipelines merge sort
            try_add_multi_sort_merge(
                pipeline,
                self.schema,
                self.final_block_size,
                self.limit,
                self.sort_desc,
                self.prof_info.clone(),
                self.remove_order_col_at_last,
            )?;
        }

        Ok(())
    }
}

fn order_column_type(desc: &[SortColumnDescription], schema: &DataSchema) -> DataType {
    debug_assert!(!desc.is_empty());
    if desc.len() == 1 {
        let order_by_field = schema.field(desc[0].offset);
        if matches!(
            order_by_field.data_type(),
            DataType::Number(_) | DataType::Date | DataType::Timestamp | DataType::String
        ) {
            return order_by_field.data_type().clone();
        }
    }
    DataType::String
}
