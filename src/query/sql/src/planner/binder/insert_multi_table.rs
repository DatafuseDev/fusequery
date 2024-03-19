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

use databend_common_ast::ast::InsertMultiTableStmt;
use databend_common_ast::ast::InsertSource;
use databend_common_ast::ast::IntoClause;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::ScalarBinder;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerContext;
use crate::plans::Else;
use crate::plans::InsertInputSource;
use crate::plans::InsertMultiTable;
use crate::plans::Into;
use crate::plans::Plan;
use crate::plans::When;
use crate::BindContext;
use crate::Binder;
impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_insert_multi_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &InsertMultiTableStmt,
    ) -> Result<Plan> {
        let InsertMultiTableStmt {
            when_clauses,
            else_clause,
            source,
        } = stmt;

        let input_source = match source {
            InsertSource::Select { query } => {
                let statement = Statement::Query(query.clone());
                let select_plan = self.bind_statement(bind_context, &statement).await?;
                let opt_ctx = OptimizerContext::new(self.ctx.clone(), self.metadata.clone())
                    .with_enable_distributed_optimization(!self.ctx.get_cluster().is_empty());

                if let Plan::Query { s_expr, .. } = &select_plan {
                    if !self.check_sexpr_top(s_expr)? {
                        return Err(ErrorCode::SemanticError(
                            "insert source can't contain udf functions".to_string(),
                        ));
                    }
                }

                let optimized_plan = optimize(opt_ctx, select_plan)?;
                InsertInputSource::SelectPlan(Box::new(optimized_plan))
            }
            _ => unreachable!(),
        };

        let mut source_bind_context = bind_context.clone();
        let mut whens = vec![];
        for when_clause in when_clauses {
            let mut scalar_binder = ScalarBinder::new(
                &mut source_bind_context,
                self.ctx.clone(),
                &self.name_resolution_ctx,
                self.metadata.clone(),
                &[],
                self.m_cte_bound_ctx.clone(),
                self.ctes_map.clone(),
            );
            let (condition, _) = scalar_binder.bind(&when_clause.condition).await?;
            let intos = self.bind_into_clauses(&when_clause.into_clauses).await?;
            whens.push(When { condition, intos });
        }

        let opt_else = match else_clause {
            Some(else_clause) => {
                let intos = self.bind_into_clauses(&else_clause.into_clauses).await?;
                Some(Else { intos })
            }
            None => None,
        };

        let plan = InsertMultiTable {
            input_source,
            whens,
            opt_else,
        };
        Ok(Plan::InsertMultiTable(Box::new(plan)))
    }
}

impl Binder {
    async fn bind_into_clauses(
        &mut self,
        into_clauses: &[IntoClause],
        // source_schema: DataSchemaRef,
    ) -> Result<Vec<Into>> {
        let mut intos = vec![];
        for into_clause in into_clauses {
            let IntoClause {
                catalog,
                database,
                table,
                target_columns,
                source_columns,
            } = into_clause;
            let (catalog_name, database_name, table_name) =
                self.normalize_object_identifier_triple(catalog, database, table);

            let target_table = self
                .ctx
                .get_table(&catalog_name, &database_name, &table_name)
                .await?;

            // let target_schema = if target_columns.is_empty() {
            //     target_table.schema()
            // } else {
            //     self.schema_project(&target_table.schema(), target_columns.as_ref())?
            // };

            // let source_schema = if source_columns.is_empty() {
            //     source_schema
            // } else {
            //     self.schema_project(source_schema.clone().into(), source_columns.as_ref())?
            // };

            // if target_schema.fields().len() != source_schema.fields().len() {
            //     return Err(ErrorCode::BadArguments(
            //             "The number of columns in the target table and the source table must be the same"
            //                 .to_string(),
            //         ));
            // }

            intos.push(Into {
                catalog: catalog_name,
                database: database_name,
                table: table_name,
                // target_schema,
                // source_schema,
            });
        }
        Ok(intos)
    }
}
