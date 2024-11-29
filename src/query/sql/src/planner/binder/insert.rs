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

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::InsertSource;
use databend_common_ast::ast::InsertStmt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;

use super::util::TableIdentifier;
use crate::binder::Binder;
use crate::executor::physical_plans::Values;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::Append;
use crate::plans::AppendType;
use crate::plans::Plan;
use crate::plans::ValueScan;
use crate::BindContext;

impl Binder {
    pub fn schema_project(
        &self,
        schema: &Arc<TableSchema>,
        columns: &[Identifier],
    ) -> Result<Arc<TableSchema>> {
        let fields = if columns.is_empty() {
            schema
                .fields()
                .iter()
                .filter(|f| f.computed_expr().is_none())
                .cloned()
                .collect::<Vec<_>>()
        } else {
            columns
                .iter()
                .map(|ident| {
                    let field = schema.field_with_name(
                        &normalize_identifier(ident, &self.name_resolution_ctx).name,
                    )?;
                    if field.computed_expr().is_some() {
                        Err(ErrorCode::BadArguments(format!(
                            "The value specified for computed column '{}' is not allowed",
                            field.name()
                        )))
                    } else {
                        Ok(field.clone())
                    }
                })
                .collect::<Result<Vec<_>>>()?
        };
        Ok(TableSchemaRefExt::create(fields))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_insert(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &InsertStmt,
    ) -> Result<Plan> {
        let InsertStmt {
            with,
            catalog,
            database,
            table,
            columns,
            source,
            overwrite,
            ..
        } = stmt;

        self.init_cte(bind_context, with)?;

        let table_identifier = TableIdentifier::new(self, catalog, database, table, &None);
        let (catalog_name, database_name, table_name) = (
            table_identifier.catalog_name(),
            table_identifier.database_name(),
            table_identifier.table_name(),
        );

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await
            .map_err(|err| table_identifier.not_found_suggest_error(err))?;

        let table_index = self.metadata.write().add_table(
            catalog_name.clone(),
            database_name.clone(),
            table.clone(),
            None,
            false,
            false,
            false,
            false,
        );

        let schema = self.schema_project(&table.schema(), columns)?;
        let schema: DataSchemaRef = Arc::new(schema.into());

        let (source, project_columns) = match source.clone() {
            InsertSource::Values { rows } => {
                let mut new_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let new_row = bind_context
                        .exprs_to_scalar(
                            &row,
                            &schema,
                            self.ctx.clone(),
                            &self.name_resolution_ctx,
                            self.metadata.clone(),
                        )
                        .await?;
                    new_rows.push(new_row);
                }
                (
                    SExpr::create_leaf(Arc::new(
                        ValueScan {
                            values: Values::Values(Arc::new(new_rows)),
                            dest_schema: schema.clone(),
                        }
                        .into(),
                    )),
                    None,
                )
            }
            InsertSource::RawValues { rest_str, start } => {
                let values_str = rest_str.trim_end_matches(';').trim_start().to_owned();
                match self.ctx.get_stage_attachment() {
                    Some(attachment) => {
                        return self
                            .bind_copy_from_attachment(
                                bind_context,
                                attachment,
                                catalog_name,
                                database_name,
                                table_name,
                                schema,
                                &values_str,
                            )
                            .await;
                    }
                    None => (
                        SExpr::create_leaf(Arc::new(
                            ValueScan {
                                values: Values::RawValues {
                                    rest_str: Arc::new(values_str),
                                    start,
                                },
                                dest_schema: schema.clone(),
                            }
                            .into(),
                        )),
                        None,
                    ),
                }
            }
            InsertSource::Select { query } => {
                let (source, bind_context) = self.bind_query(bind_context, &query)?;
                (source, Some(bind_context.columns.clone()))
            }
        };

        let copy_into = Append {
            table_index,
            required_values_schema: schema.clone(),
            values_consts: vec![],
            required_source_schema: schema,
            append_type: AppendType::Insert,
            project_columns,
        };

        Ok(Plan::Append {
            s_expr: Box::new(SExpr::create_unary(
                Arc::new(copy_into.into()),
                Arc::new(source),
            )),
            metadata: self.metadata.clone(),
            stage_table_info: None,
            overwrite: *overwrite,
            forbid_occ_retry: false,
        })
    }
}
