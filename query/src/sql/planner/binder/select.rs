// Copyright 2022 Datafuse Labs.
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

use async_recursion::async_recursion;
use common_ast::ast::Expr;
use common_ast::ast::Query;
use common_ast::ast::SelectStmt;
use common_ast::ast::SetExpr;
use common_ast::ast::TableReference;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::Expression;

use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::scalar::ScalarBinder;
use crate::sql::planner::binder::BindContext;
use crate::sql::planner::binder::Binder;
use crate::sql::planner::binder::ColumnBinding;
use crate::sql::plans::FilterPlan;
use crate::sql::plans::LogicalGet;
use crate::sql::plans::Scalar;
use crate::sql::IndexType;
use crate::sql::ScalarExprRef;
use crate::storages::Table;
use crate::storages::ToReadDataSourcePlan;
use crate::table_functions::TableFunction;

impl Binder {
    #[async_recursion]
    pub(super) async fn bind_query(
        &mut self,
        query: &Query,
        bind_context: &BindContext,
    ) -> Result<BindContext> {
        match &query.body {
            SetExpr::Select(stmt) => self.bind_select_stmt(stmt, bind_context).await,
            SetExpr::Query(stmt) => self.bind_query(stmt, bind_context).await,
            _ => todo!(),
        }
        // TODO: support ORDER BY
    }

    pub(super) async fn bind_select_stmt(
        &mut self,
        stmt: &SelectStmt,
        bind_context: &BindContext,
    ) -> Result<BindContext> {
        let mut input_context = if let Some(from) = &stmt.from {
            self.bind_table_reference(from, bind_context).await?
        } else {
            BindContext::create()
        };

        if let Some(expr) = &stmt.selection {
            self.bind_where(expr, &mut input_context, false)?;
        }

        // Output of current `SELECT` statement.
        let mut output_context = self.normalize_select_list(&stmt.select_list, &input_context)?;

        self.analyze_aggregate(&output_context, &mut input_context)?;

        if !input_context.agg_scalar_exprs.as_ref().unwrap().is_empty() || !stmt.group_by.is_empty()
        {
            self.bind_group_by(&stmt.group_by, &mut input_context)?;
            output_context.expression = input_context.expression.clone();
        }

        if let Some(expr) = &stmt.having {
            self.bind_where(expr, &mut input_context, true)?;
            output_context.expression = input_context.expression.clone();
        }

        self.bind_projection(&mut output_context)?;

        Ok(output_context)
    }

    async fn bind_table_reference(
        &mut self,
        stmt: &TableReference,
        bind_context: &BindContext,
    ) -> Result<BindContext> {
        match stmt {
            TableReference::Table {
                database,
                table,
                alias,
            } => {
                let database = database
                    .as_ref()
                    .map(|ident| ident.name.clone())
                    .unwrap_or_else(|| self.ctx.get_current_database());
                let table = table.name.clone();
                // TODO: simply normalize table name to lower case, maybe use a more reasonable way
                let table = table.to_lowercase();
                let tenant = self.ctx.get_tenant();

                // Resolve table with catalog
                let table_meta: Arc<dyn Table> = self
                    .resolve_data_source(tenant.as_str(), database.as_str(), table.as_str())
                    .await?;
                let source = table_meta.read_plan(self.ctx.clone(), None).await?;
                let table_index = self.metadata.add_table(database, table_meta, source);

                let mut result = self.bind_base_table(table_index).await?;
                if let Some(alias) = alias {
                    result.apply_table_alias(&table, alias)?;
                }
                Ok(result)
            }
            TableReference::TableFunction {
                name,
                params,
                alias,
            } => {
                let scalar_binder = ScalarBinder::new();
                let args = params
                    .iter()
                    .map(|arg| scalar_binder.bind_expr(arg, bind_context))
                    .collect::<Result<Vec<ScalarExprRef>>>()?;
                let expressions = args
                    .into_iter()
                    .map(|scalar| {
                        let scalar = scalar.as_any().downcast_ref::<Scalar>().unwrap();
                        match scalar {
                            Scalar::Literal { data_value } => Ok(Expression::Literal {
                                value: data_value.clone(),
                                column_name: None,
                                data_type: data_value.data_type(),
                            }),
                            _ => Err(ErrorCode::UnImplement(format!(
                                "Unsupported table argument type: {:?}",
                                scalar
                            ))),
                        }
                    })
                    .collect::<Result<Vec<Expression>>>()?;

                let table_args = Some(expressions);

                let table_meta: Arc<dyn TableFunction> = self
                    .catalog
                    .get_table_function(name.name.as_str(), table_args)?;
                let table = table_meta.as_table();

                let source = table.read_plan(self.ctx.clone(), None).await?;
                let table_index =
                    self.metadata
                        .add_table("system".to_string(), table.clone(), source);

                let mut result = self.bind_base_table(table_index).await?;
                if let Some(alias) = alias {
                    result.apply_table_alias(table.name(), alias)?;
                }
                Ok(result)
            }
            _ => todo!(),
        }
    }

    async fn bind_base_table(&mut self, table_index: IndexType) -> Result<BindContext> {
        let mut bind_context = BindContext::create();
        let columns = self.metadata.columns_by_table_index(table_index);
        let table = self.metadata.table(table_index);
        for column in columns.iter() {
            let column_binding = ColumnBinding {
                table_name: Some(table.name.clone()),
                column_name: column.name.clone(),
                index: column.column_index,
                data_type: column.data_type.clone(),
                nullable: column.nullable,
                scalar: None,
            };
            bind_context.add_column_binding(column_binding);
        }
        bind_context.expression = Some(SExpr::create_leaf(
            LogicalGet {
                table_index,
                columns: columns.into_iter().map(|col| col.column_index).collect(),
            }
            .into(),
        ));

        Ok(bind_context)
    }

    pub(super) fn bind_where(
        &mut self,
        expr: &Expr,
        bind_context: &mut BindContext,
        is_having: bool,
    ) -> Result<()> {
        let scalar_binder = ScalarBinder::new();
        let scalar = scalar_binder.bind_expr(expr, bind_context)?;
        let filter_plan = FilterPlan {
            predicate: scalar,
            is_having,
        };
        let new_expr =
            SExpr::create_unary(filter_plan.into(), bind_context.expression.clone().unwrap());
        bind_context.expression = Some(new_expr);
        Ok(())
    }
}
