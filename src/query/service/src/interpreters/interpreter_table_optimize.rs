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
use std::time::SystemTime;

use common_base::runtime::GlobalIORuntime;
use common_catalog::table::CompactTarget;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_pipeline_core::Pipeline;
use common_sql::plans::OptimizeTableAction;
use common_sql::plans::OptimizeTablePlan;
use common_storages_factory::NavigationPoint;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct OptimizeTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
}

impl OptimizeTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: OptimizeTablePlan) -> Result<Self> {
        Ok(OptimizeTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for OptimizeTableInterpreter {
    fn name(&self) -> &str {
        "OptimizeTableInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let ctx = self.ctx.clone();
        let plan = self.plan.clone();
        match self.plan.action.clone() {
            OptimizeTableAction::CompactBlocks => {
                self.build_pipeline(CompactTarget::Blocks, false).await
            }
            OptimizeTableAction::CompactSegments => {
                self.build_pipeline(CompactTarget::Segments, false).await
            }
            OptimizeTableAction::Purge(point) => {
                purge(ctx, plan, point).await?;
                Ok(PipelineBuildResult::create())
            }
            OptimizeTableAction::All => self.build_pipeline(CompactTarget::Blocks, true).await,
        }
    }
}

impl OptimizeTableInterpreter {
    pub async fn build_compact_pipeline(
        ctx: &Arc<QueryContext>,
        mut table: Arc<dyn Table>,
        target: CompactTarget,
    ) -> Result<PipelineBuildResult> {
        let need_recluster = !table.cluster_keys(ctx.clone()).is_empty();
        // check if the table is locked.
        let mut pipeline = Pipeline::create();
        table
            .compact(ctx.clone(), target, None, &mut pipeline)
            .await?;

        let mut build_res = PipelineBuildResult::create();
        let settings = ctx.get_settings();
        if need_recluster {
            if !pipeline.is_empty() {
                pipeline.set_max_threads(settings.get_max_threads()? as usize);

                let query_id = ctx.get_id();
                let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
                let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;

                ctx.set_executor(executor.get_inner())?;
                executor.execute()?;

                // refresh table.
                table = table.as_ref().refresh(ctx.as_ref()).await?;
            }

            table
                .recluster(ctx.clone(), None, None, &mut build_res.main_pipeline)
                .await?;
        } else {
            build_res.main_pipeline = pipeline;
        }

        Ok(build_res)
    }

    async fn build_pipeline(
        &self,
        target: CompactTarget,
        need_purge: bool,
    ) -> Result<PipelineBuildResult> {
        let mut table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        let need_recluster = !table.cluster_keys(self.ctx.clone()).is_empty();

        // check if the table is locked.
        let catalog = self.ctx.get_catalog(&self.plan.catalog)?;
        let reply = catalog
            .list_table_lock_revs(table.get_table_info().ident.table_id)
            .await?;
        if !reply.is_empty() {
            return Err(ErrorCode::TableAlreadyLocked(format!(
                "table '{}' is locked, please retry compaction later",
                self.plan.table
            )));
        }

        let mut compact_pipeline = Pipeline::create();
        table
            .compact(
                self.ctx.clone(),
                target,
                self.plan.limit,
                &mut compact_pipeline,
            )
            .await?;

        let mut build_res = PipelineBuildResult::create();
        let settings = self.ctx.get_settings();
        if need_recluster {
            if !compact_pipeline.is_empty() {
                compact_pipeline.set_max_threads(settings.get_max_threads()? as usize);

                let query_id = self.ctx.get_id();
                let executor_settings = ExecutorSettings::try_create(&settings, query_id)?;
                let executor =
                    PipelineCompleteExecutor::try_create(compact_pipeline, executor_settings)?;

                self.ctx.set_executor(executor.get_inner())?;
                executor.execute()?;

                // refresh table.
                table = table.as_ref().refresh(self.ctx.as_ref()).await?;
            }

            table
                .recluster(
                    self.ctx.clone(),
                    None,
                    self.plan.limit,
                    &mut build_res.main_pipeline,
                )
                .await?;
        } else {
            build_res.main_pipeline = compact_pipeline;
        }

        let ctx = self.ctx.clone();
        let plan = self.plan.clone();
        if build_res.main_pipeline.is_empty() {
            if need_purge {
                purge(ctx, plan, None).await?;
            }
        } else {
            let start = SystemTime::now();
            build_res
                .main_pipeline
                .set_on_finished(move |may_error| match may_error {
                    None => {
                        if need_recluster {
                            InterpreterClusteringHistory::write_log(
                                &ctx,
                                start,
                                &plan.database,
                                &plan.table,
                            )?;
                        }
                        if need_purge {
                            GlobalIORuntime::instance()
                                .block_on(async move { purge(ctx, plan, None).await })?;
                        }
                        Ok(())
                    }
                    Some(error_code) => Err(error_code.clone()),
                });
        }

        Ok(build_res)
    }
}

async fn purge(
    ctx: Arc<QueryContext>,
    plan: OptimizeTablePlan,
    instant: Option<NavigationPoint>,
) -> Result<()> {
    // currently, context caches the table, we have to "refresh"
    // the table by using the catalog API directly
    let table = ctx
        .get_catalog(&plan.catalog)?
        .get_table(ctx.get_tenant().as_str(), &plan.database, &plan.table)
        .await?;

    let keep_latest = true;
    let res = table
        .purge(ctx, instant, plan.limit, keep_latest, false)
        .await?;
    assert!(res.is_none());
    Ok(())
}
