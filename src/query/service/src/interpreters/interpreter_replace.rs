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
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_pipeline_sources::AsyncSourcer;
use common_sql::plans::InsertInputSource;
use common_sql::plans::OptimizeTableAction;
use common_sql::plans::OptimizeTablePlan;
use common_sql::plans::Plan;
use common_sql::plans::Replace;
use common_sql::NameResolutionContext;
use tracing::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::fill_missing_columns;
use crate::interpreters::interpreter_copy::CopyInterpreter;
use crate::interpreters::interpreter_insert::ValueSource;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::OptimizeTableInterpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[allow(dead_code)]
pub struct ReplaceInterpreter {
    ctx: Arc<QueryContext>,
    plan: Replace,
}

impl ReplaceInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Replace) -> Result<InterpreterPtr> {
        Ok(Arc::new(ReplaceInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for ReplaceInterpreter {
    fn name(&self) -> &str {
        "ReplaceIntoInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        self.check_on_conflicts()?;

        let plan = &self.plan;
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let has_cluster_key = !table.cluster_keys(self.ctx.clone()).is_empty();

        let mut pipeline = self
            .connect_input_source(self.ctx.clone(), &self.plan.source, self.plan.schema())
            .await?;

        if pipeline.main_pipeline.is_empty() {
            return Ok(pipeline);
        }

        fill_missing_columns(
            self.ctx.clone(),
            table.clone(),
            self.plan.schema(),
            &mut pipeline.main_pipeline,
        )?;

        let on_conflict_fields = plan.on_conflict_fields.clone();
        table
            .replace_into(
                self.ctx.clone(),
                &mut pipeline.main_pipeline,
                on_conflict_fields,
            )
            .await?;

        if !pipeline.main_pipeline.is_empty()
            && has_cluster_key
            && self.ctx.get_settings().get_enable_auto_reclustering()?
        {
            let ctx = self.ctx.clone();
            let catalog = self.plan.catalog.clone();
            let database = self.plan.database.to_string();
            let table = self.plan.table.to_string();
            pipeline.main_pipeline.set_on_finished(|err| {
                    if err.is_none() {
                        info!("execute replace into finished successfully. running table optimization job.");
                         match  GlobalIORuntime::instance().block_on({
                             async move {
                                 let optimize_interpreter = OptimizeTableInterpreter::try_create(ctx.clone(),
                                 OptimizeTablePlan {
                                     catalog,
                                     database,
                                     table,
                                     action: OptimizeTableAction::CompactBlocks,
                                     limit: None,
                                 }
                                 )?;

                                 let mut build_res = optimize_interpreter.execute2().await?;

                                 if build_res.main_pipeline.is_empty() {
                                     return Ok(());
                                 }

                                 let settings = ctx.get_settings();
                                 let query_id = ctx.get_id();
                                 build_res.set_max_threads(settings.get_max_threads()? as usize);
                                 let settings = ExecutorSettings::try_create(&settings, query_id)?;

                                 if build_res.main_pipeline.is_complete_pipeline()? {
                                     let mut pipelines = build_res.sources_pipelines;
                                     pipelines.push(build_res.main_pipeline);

                                     let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

                                     ctx.set_executor(complete_executor.get_inner())?;
                                     complete_executor.execute()?;
                                 }
                                 Ok(())
                             }
                         }) {
                            Ok(_) => {
                                info!("execute replace into finished successfully. table optimization job finished.");
                            }
                            Err(e) => { info!("execute replace into finished successfully. table optimization job failed. {:?}", e)}
                        }

                        return Ok(());
                    }
                    Ok(())
                });
        }
        Ok(pipeline)
    }
}

impl ReplaceInterpreter {
    fn check_on_conflicts(&self) -> Result<()> {
        if self.plan.on_conflict_fields.is_empty() {
            Err(ErrorCode::BadArguments(
                "at least one column must be specified in the replace into .. on [conflict] statement",
            ))
        } else {
            Ok(())
        }
    }
    #[async_backtrace::framed]
    async fn connect_input_source<'a>(
        &'a self,
        ctx: Arc<QueryContext>,
        source: &'a InsertInputSource,
        schema: DataSchemaRef,
    ) -> Result<PipelineBuildResult> {
        match source {
            InsertInputSource::Values(data) => {
                self.connect_value_source(ctx.clone(), schema.clone(), data)
            }

            InsertInputSource::SelectPlan(plan) => {
                self.connect_query_plan_source(ctx.clone(), schema.clone(), plan)
                    .await
            }
            InsertInputSource::Stage(plan) => match *plan.clone() {
                Plan::Copy(copy_plan) => {
                    let interpreter = CopyInterpreter::try_create(ctx.clone(), *copy_plan.clone())?;
                    interpreter.execute2().await
                }
                _ => unreachable!("plan in InsertInputSource::Stag must be Copy"),
            },
            _ => Err(ErrorCode::Unimplemented(
                "input source other than literal VALUES and sub queries are NOT supported yet.",
            )),
        }
    }

    fn connect_value_source(
        &self,
        ctx: Arc<QueryContext>,
        schema: DataSchemaRef,
        value_data: &str,
    ) -> Result<PipelineBuildResult> {
        let mut build_res = PipelineBuildResult::create();
        let settings = ctx.get_settings();
        build_res.main_pipeline.add_source(
            |output| {
                let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
                let inner = ValueSource::new(
                    value_data.to_string(),
                    ctx.clone(),
                    name_resolution_ctx,
                    schema.clone(),
                );
                AsyncSourcer::create(ctx.clone(), output, inner)
            },
            1,
        )?;
        Ok(build_res)
    }

    #[async_backtrace::framed]
    async fn connect_query_plan_source<'a>(
        &'a self,
        ctx: Arc<QueryContext>,
        self_schema: DataSchemaRef,
        query_plan: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query_plan {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                formatted_ast,
                ..
            } => (s_expr, metadata, bind_context, formatted_ast),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let select_interpreter = SelectInterpreter::try_create(
            ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        let mut build_res = select_interpreter.execute2().await?;

        let select_schema = query_plan.schema();
        let target_schema = self_schema;
        if self.check_schema_cast(query_plan)? {
            let func_ctx = ctx.get_function_context()?;
            build_res.main_pipeline.add_transform(
                |transform_input_port, transform_output_port| {
                    TransformCastSchema::try_create(
                        transform_input_port,
                        transform_output_port,
                        select_schema.clone(),
                        target_schema.clone(),
                        func_ctx.clone(),
                    )
                },
            )?;
        }

        Ok(build_res)
    }

    // TODO duplicated
    fn check_schema_cast(&self, plan: &Plan) -> Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan.schema();

        // validate schema
        if select_schema.fields().len() < output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(
                "Fields in select statement is less than expected",
            ));
        }

        // check if cast needed
        let cast_needed = select_schema != DataSchema::from(output_schema.as_ref()).into();
        Ok(cast_needed)
    }
}
