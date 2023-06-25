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

use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::DataSourcePlan;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::infer_table_schema;
use common_expression::DataField;
use common_expression::DataSchemaRefExt;
use common_expression::BLOCK_NAME_COL_NAME;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::UpdateIndexReq;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_sql::evaluator::BlockOperator;
use common_sql::evaluator::CompoundBlockOperator;
use common_sql::executor::PhysicalPlan;
use common_sql::executor::PhysicalPlanBuilder;
use common_sql::executor::PhysicalPlanReplacer;
use common_sql::plans::Plan;
use common_sql::plans::RefreshIndexPlan;
use common_sql::plans::RelOperator;
use common_storages_fuse::operations::AggIndexSink;
use common_storages_fuse::FusePartInfo;
use common_storages_fuse::FuseTable;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::schedulers::ReplaceReadSource;
use crate::sessions::QueryContext;

pub struct RefreshIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshIndexPlan,
}

impl RefreshIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshIndexPlan) -> Result<Self> {
        Ok(RefreshIndexInterpreter { ctx, plan })
    }

    fn get_read_source(&self, query_plan: &PhysicalPlan) -> Result<DataSourcePlan> {
        let mut source = vec![];

        let mut collect_read_source = |plan: &PhysicalPlan| {
            if let PhysicalPlan::TableScan(scan) = plan {
                source.push(*scan.source.clone())
            }
        };

        PhysicalPlan::traverse(
            query_plan,
            &mut |_| true,
            &mut collect_read_source,
            &mut |_| {},
        );

        if source.len() != 1 {
            Err(ErrorCode::Internal(
                "Invalid source with multiple table scan when do refresh aggregating index"
                    .to_string(),
            ))
        } else {
            let mut source = source.remove(0);

            // first, sort the partitions by create_on.
            source.parts.partitions.sort_by(|p1, p2| {
                let p1 = FusePartInfo::from_part(p1).unwrap();
                let p2 = FusePartInfo::from_part(p2).unwrap();
                p1.create_on.partial_cmp(&p2.create_on).unwrap()
            });

            // then, find the last refresh position.
            let last = match source.parts.partitions.binary_search_by(|p| {
                let fp = FusePartInfo::from_part(p).unwrap();
                fp.create_on
                    .partial_cmp(&self.plan.index_meta.update_on)
                    .unwrap()
            }) {
                Ok(i) => i + 1,
                Err(i) => i,
            };

            // finally, skip the refreshed partitions.
            source.parts.partitions = source.parts.partitions.iter().skip(last).cloned().collect();

            Ok(source)
        }
    }

    fn update_index_meta(&self, read_source: &DataSourcePlan) -> Result<IndexMeta> {
        let fuse_part = FusePartInfo::from_part(read_source.parts.partitions.last().unwrap())?;
        let mut index_meta = self.plan.index_meta.clone();
        index_meta.update_on = fuse_part.create_on;
        Ok(index_meta)
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshIndexInterpreter {
    fn name(&self) -> &str {
        "RefreshIndexInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let (mut query_plan, output_schema, select_columns) = match self.plan.query_plan.as_ref() {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let schema = if let RelOperator::EvalScalar(eval) = s_expr.plan() {
                    let fields = eval
                        .items
                        .iter()
                        .map(|item| {
                            let ty = item.scalar.data_type()?;
                            Ok(DataField::new(&item.index.to_string(), ty))
                        })
                        .collect::<Result<Vec<_>>>()?;
                    DataSchemaRefExt::create(fields)
                } else {
                    return Err(ErrorCode::SemanticError(
                        "The last operator of the plan of aggregate index query should be EvalScalar",
                    ));
                };

                let mut builder =
                    PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                (
                    builder.build(s_expr.as_ref()).await?,
                    schema,
                    bind_context.columns.clone(),
                )
            }
            _ => {
                return Err(ErrorCode::SemanticError(
                    "Refresh aggregating index encounter Non-Query Plan",
                ));
            }
        };

        let new_read_source = self.get_read_source(&query_plan)?;
        let new_index_meta = self.update_index_meta(&new_read_source)?;

        let mut replace_read_source = ReplaceReadSource {
            source: new_read_source,
        };
        query_plan = replace_read_source.replace(&query_plan)?;

        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &query_plan, false).await?;

        let input_schema = query_plan.output_schema()?;

        // Build projection
        let mut projections = Vec::with_capacity(output_schema.num_fields());
        for field in output_schema.fields().iter() {
            let index = input_schema.index_of(field.name())?;
            projections.push(index);
        }
        let num_input_columns = input_schema.num_fields();
        let func_ctx = self.ctx.get_function_context()?;
        build_res.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                num_input_columns,
                func_ctx.clone(),
                vec![BlockOperator::Project {
                    projection: projections.clone(),
                }],
            )))
        })?;

        // Find the block name column offset in the block.
        let block_name_col = select_columns
            .iter()
            .find(|col| col.column_name.eq_ignore_ascii_case(BLOCK_NAME_COL_NAME))
            .ok_or_else(|| {
                ErrorCode::Internal(
                    "_block_name should contained in the input of refresh processor",
                )
            })?;
        let block_name_offset = output_schema.index_of(&block_name_col.index.to_string())?;

        // Build the final sink schema.
        let mut sink_schema = infer_table_schema(&output_schema)?.as_ref().clone();
        if !self.plan.user_defined_block_name {
            sink_schema.drop_column(&block_name_col.index.to_string())?;
        }
        let sink_schema = Arc::new(sink_schema);

        let data_accessor = self.ctx.get_data_operator()?;
        let fuse_table = FuseTable::do_create(self.plan.table_info.clone())?;
        let fuse_table: Arc<FuseTable> = fuse_table.into();
        let write_settings = fuse_table.get_write_settings();

        build_res.main_pipeline.try_resize(1)?;
        build_res.main_pipeline.add_sink(|input| {
            AggIndexSink::try_create(
                input,
                data_accessor.operator(),
                self.plan.index_id,
                write_settings.clone(),
                sink_schema.clone(),
                block_name_offset,
                self.plan.user_defined_block_name,
            )
        })?;

        let ctx = self.ctx.clone();
        let req = UpdateIndexReq {
            index_id: self.plan.index_id,
            index_meta: new_index_meta,
        };

        build_res
            .main_pipeline
            .set_on_finished(move |may_error| match may_error {
                None => GlobalIORuntime::instance()
                    .block_on(async move { modify_last_update(ctx, req).await }),
                Some(error_code) => Err(error_code.clone()),
            });

        return Ok(build_res);
    }
}

async fn modify_last_update(ctx: Arc<QueryContext>, req: UpdateIndexReq) -> Result<()> {
    let catalog = ctx.get_catalog(&ctx.get_current_catalog())?;
    catalog.update_index(req).await?;
    Ok(())
}
