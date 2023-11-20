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

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TableNameIdent;
use common_sql::plans::CreateViewPlan;
use common_sql::plans::Plan;
use common_sql::Planner;
use common_storages_view::view_table::QUERY;
use common_storages_view::view_table::VIEW_ENGINE;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct CreateViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateViewPlan,
}

impl CreateViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateViewPlan) -> Result<Self> {
        Ok(CreateViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateViewInterpreter {
    fn name(&self) -> &str {
        "CreateViewInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let tenant = self.ctx.get_tenant();
        let table_function = catalog.list_table_functions();
        if !self.plan.if_not_exists
            && catalog
                .exists_table(tenant.as_str(), &self.plan.database, &self.plan.view_name)
                .await?
        {
            return Err(ErrorCode::ViewAlreadyExists(format!(
                "{}.{} as view Already Exists",
                self.plan.database, self.plan.view_name
            )));
        }
        let mut options = BTreeMap::new();
        let mut planner = Planner::new(self.ctx.clone());
        let (plan, _) = planner.plan_sql(&self.plan.subquery.clone()).await?;
        match plan.clone() {
            Plan::Query { metadata, .. } => {
                let metadata = metadata.read().clone();
                for table in metadata.tables() {
                    let database_name = table.database();
                    let table_name = table.name();
                    if !catalog
                        .exists_table(tenant.as_str(), database_name, table_name)
                        .await?
                        && !table_function.contains(&table_name.to_string())
                        && !table.table().is_stage_table()
                    {
                        return Err(common_exception::ErrorCode::UnknownTable(format!(
                            "VIEW QUERY: table `{}`.`{}` not exists in catalog '{}'",
                            database_name,
                            table_name,
                            &catalog.name()
                        )));
                    }
                }
            }
            _ => {
                // This logic will never be used, because of QUERY parse as query
                return Err(ErrorCode::Unimplemented("create view only support Query"));
            }
        }

        let subquery = if self.plan.column_names.is_empty() {
            self.plan.subquery.clone()
        } else {
            if plan.schema().fields().len() != self.plan.column_names.len() {
                return Err(ErrorCode::BadDataArrayLength(format!(
                    "column name length mismatch, expect {}, got {}",
                    plan.schema().fields().len(),
                    self.plan.column_names.len(),
                )));
            }
            format!(
                "select * from ({}) {}({})",
                self.plan.subquery,
                self.plan.view_name,
                self.plan.column_names.join(", ")
            )
        };
        options.insert(QUERY.to_string(), subquery);

        let plan = CreateTableReq {
            if_not_exists: self.plan.if_not_exists,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.clone(),
                table_name: self.plan.view_name.clone(),
            },
            table_meta: TableMeta {
                engine: VIEW_ENGINE.to_string(),
                options,
                ..Default::default()
            },
        };
        catalog.create_table(plan).await?;

        Ok(PipelineBuildResult::create())
    }
}
