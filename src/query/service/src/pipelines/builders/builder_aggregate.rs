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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::HashTableConfig;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_pipeline_transforms::processors::TransformSortPartial;
use databend_common_sql::executor::physical_plans::AggregateExpand;
use databend_common_sql::executor::physical_plans::AggregateFinal;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::executor::physical_plans::AggregatePartial;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::IndexType;
use databend_common_storage::DataOperator;

use crate::pipelines::processors::transforms::aggregator::build_partition_bucket;
use crate::pipelines::processors::transforms::aggregator::AggregateInjector;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::FinalSingleStateAggregator;
use crate::pipelines::processors::transforms::aggregator::PartialSingleStateAggregator;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillWriter;
use crate::pipelines::processors::transforms::aggregator::TransformExpandGroupingSets;
use crate::pipelines::processors::transforms::aggregator::TransformPartialAggregate;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_aggregate_expand(&mut self, expand: &AggregateExpand) -> Result<()> {
        self.build_pipeline(&expand.input)?;
        let input_schema = expand.input.output_schema()?;
        let group_bys = expand
            .group_bys
            .iter()
            .take(expand.group_bys.len() - 1) // The last group-by will be virtual column `_grouping_id`
            .map(|i| input_schema.index_of(&i.to_string()))
            .collect::<Result<Vec<_>>>()?;
        let grouping_sets = expand
            .grouping_sets
            .sets
            .iter()
            .map(|sets| {
                sets.iter()
                    .map(|i| {
                        let i = input_schema.index_of(&i.to_string())?;
                        let offset = group_bys.iter().position(|j| *j == i).unwrap();
                        Ok(offset)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .collect::<Result<Vec<_>>>()?;
        let mut grouping_ids = Vec::with_capacity(grouping_sets.len());
        let mask = (1 << group_bys.len()) - 1;
        for set in grouping_sets {
            let mut id = 0;
            for i in set {
                id |= 1 << i;
            }
            // For element in `group_bys`,
            // if it is in current grouping set: set 0, else: set 1. (1 represents it will be NULL in grouping)
            // Example: GROUP BY GROUPING SETS ((a, b), (a), (b), ())
            // group_bys: [a, b]
            // grouping_sets: [[0, 1], [0], [1], []]
            // grouping_ids: 00, 01, 10, 11
            grouping_ids.push(!id & mask);
        }

        self.main_pipeline.add_transformer(|| {
            TransformExpandGroupingSets::new(group_bys.clone(), grouping_ids.clone())
        });
        Ok(())
    }

    pub(crate) fn build_aggregate_partial(&mut self, aggregate: &AggregatePartial) -> Result<()> {
        self.contain_sink_processor = true;
        self.build_pipeline(&aggregate.input)?;

        let max_block_size = self.settings.get_max_block_size()?;
        let max_threads = self.settings.get_max_threads()?;
        let max_spill_io_requests = self.settings.get_max_spill_io_requests()?;

        let enable_experimental_aggregate_hashtable = self
            .settings
            .get_enable_experimental_aggregate_hashtable()?;

        let predicate = aggregate
            .pushdown_filter
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .try_reduce(|lhs, rhs| {
                check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
            })?;

        let params = Self::build_aggregator_params(
            aggregate.input.output_schema()?,
            &aggregate.group_by,
            &aggregate.agg_funcs,
            enable_experimental_aggregate_hashtable,
            self.is_exchange_neighbor,
            max_block_size as usize,
            max_spill_io_requests as usize,
            predicate,
        )?;

        if params.group_columns.is_empty() {
            let ctx = self.ctx.get_function_context()?;
            return self.main_pipeline.try_add_accumulating_transformer(|| {
                PartialSingleStateAggregator::try_new(&params, ctx.clone())
            });
        }

        let schema_before_group_by = params.input_schema.clone();

        // Need a global atomic to read the max current radix bits hint
        let partial_agg_config = if !self.is_exchange_neighbor {
            HashTableConfig::default().with_partial(true, max_threads as usize)
        } else {
            HashTableConfig::default()
                .cluster_with_partial(true, self.ctx.get_cluster().nodes.len())
        };

        // For rank limit, we can filter data using sort with rank before partial
        if let Some(rank_limit) = &aggregate.rank_limit {
            let sort_desc = rank_limit
                .0
                .iter()
                .map(|desc| {
                    let offset = schema_before_group_by.index_of(&desc.order_by.to_string())?;
                    Ok(SortColumnDescription {
                        offset,
                        asc: desc.asc,
                        nulls_first: desc.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let sort_desc = Arc::new(sort_desc);

            self.main_pipeline.add_transformer(|| {
                TransformSortPartial::new(LimitType::LimitRank(rank_limit.1), sort_desc.clone())
            });
        }

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformPartialAggregate::try_create(
                self.ctx.clone(),
                input,
                output,
                params.clone(),
                partial_agg_config.clone(),
            )?))
        })?;

        // If cluster mode, spill write will be completed in exchange serialize, because we need scatter the block data first
        if !self.is_exchange_neighbor {
            let operator = DataOperator::instance().operator();
            let location_prefix =
                query_spill_prefix(self.ctx.get_tenant().tenant_name(), &self.ctx.get_id());
            self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(TransformAggregateSpillWriter::create(
                    self.ctx.clone(),
                    input,
                    output,
                    operator.clone(),
                    params.clone(),
                    location_prefix.clone(),
                )))
            })?;
        }

        self.exchange_injector = AggregateInjector::create(self.ctx.clone(), params.clone());
        Ok(())
    }

    pub(crate) fn build_aggregate_final(&mut self, aggregate: &AggregateFinal) -> Result<()> {
        let max_block_size = self.settings.get_max_block_size()?;
        let enable_experimental_aggregate_hashtable = self
            .settings
            .get_enable_experimental_aggregate_hashtable()?;
        let max_spill_io_requests = self.settings.get_max_spill_io_requests()?;

        let params = Self::build_aggregator_params(
            aggregate.before_group_by_schema.clone(),
            &aggregate.group_by,
            &aggregate.agg_funcs,
            enable_experimental_aggregate_hashtable,
            self.is_exchange_neighbor,
            max_block_size as usize,
            max_spill_io_requests as usize,
            None,
        )?;

        if params.group_columns.is_empty() {
            self.build_pipeline(&aggregate.input)?;
            self.main_pipeline.try_resize(1)?;
            self.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(
                    FinalSingleStateAggregator::try_create(input, output, &params)?,
                ))
            })?;

            return Ok(());
        }

        let old_inject = self.exchange_injector.clone();

        let input: &PhysicalPlan = &aggregate.input;
        if matches!(input, PhysicalPlan::ExchangeSource(_)) {
            self.exchange_injector = AggregateInjector::create(self.ctx.clone(), params.clone());
        }
        self.build_pipeline(&aggregate.input)?;
        self.exchange_injector = old_inject;
        build_partition_bucket(&mut self.main_pipeline, params.clone())
    }

    pub fn build_aggregator_params(
        input_schema: DataSchemaRef,
        group_by: &[IndexType],
        agg_funcs: &[AggregateFunctionDesc],
        enable_experimental_aggregate_hashtable: bool,
        cluster_aggregator: bool,
        max_block_size: usize,
        max_spill_io_requests: usize,
        pushdown_filter: Option<Expr>,
    ) -> Result<Arc<AggregatorParams>> {
        let mut agg_args = Vec::with_capacity(agg_funcs.len());
        let (group_by, group_data_types) = group_by
            .iter()
            .map(|i| {
                let index = input_schema.index_of(&i.to_string())?;
                Ok((index, input_schema.field(index).data_type().clone()))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .unzip::<_, _, Vec<_>, Vec<_>>();

        let aggs: Vec<AggregateFunctionRef> = agg_funcs
            .iter()
            .map(|agg_func| {
                let args = agg_func
                    .arg_indices
                    .iter()
                    .map(|i| {
                        let index = input_schema.index_of(&i.to_string())?;
                        Ok(index)
                    })
                    .collect::<Result<Vec<_>>>()?;
                agg_args.push(args);
                AggregateFunctionFactory::instance().get(
                    agg_func.sig.name.as_str(),
                    agg_func.sig.params.clone(),
                    agg_func.sig.args.clone(),
                )
            })
            .collect::<Result<_>>()?;

        let params = AggregatorParams::try_create(
            input_schema,
            group_data_types,
            &group_by,
            &aggs,
            &agg_args,
            enable_experimental_aggregate_hashtable,
            cluster_aggregator,
            max_block_size,
            max_spill_io_requests,
            pushdown_filter,
        )?;

        Ok(params)
    }
}
