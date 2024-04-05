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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::DeleteStmt;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::TableReference;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ROW_ID_COL_NAME;

use crate::binder::Binder;
use crate::binder::ScalarBinder;
use crate::binder::INTERNAL_COLUMN_FACTORY;
use crate::optimizer::SExpr;
use crate::optimizer::SubqueryRewriter;
use crate::plans::BoundColumnRef;
use crate::plans::DeletePlan;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::Operator;
use crate::plans::Plan;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::RelOperator::Filter as RelOperatorFilter;
use crate::plans::RelOperator::Scan;
use crate::plans::SubqueryDesc;
use crate::plans::SubqueryExpr;
use crate::plans::Visitor;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::ScalarExpr;

impl<'a> Binder {
    pub(in crate::planner::binder) async fn process_selection(
        &self,
        filter: &'a Option<Expr>,
        table_expr: SExpr,
        scalar_binder: &mut ScalarBinder<'_>,
    ) -> Result<(Option<ScalarExpr>, Option<SubqueryDesc>)> {
        if let Some(expr) = filter {
            let (scalar, _) = scalar_binder.bind(expr).await?;
            if !self.has_subquery_in_selection(&scalar)? {
                return Ok((Some(scalar), None));
            }
            let subquery_desc = self.process_subquery(scalar.clone(), table_expr).await?;
            Ok((Some(scalar), Some(subquery_desc)))
        } else {
            Ok((None, None))
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_delete(
        &mut self,
        bind_context: &mut BindContext,
        stamt: &DeleteStmt,
    ) -> Result<Plan> {
        let DeleteStmt {
            table, selection, ..
        } = stamt;

        let (catalog_name, database_name, table_name) = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table
        {
            self.normalize_object_identifier_triple(catalog, database, table)
        } else {
            // we do not support USING clause yet
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };

        let (table_expr, mut context) = self.bind_single_table(bind_context, table).await?;

        context.allow_internal_columns(false);
        let mut scalar_binder = ScalarBinder::new(
            &mut context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );

        let (selection, subquery_desc) = self
            .process_selection(selection, table_expr, &mut scalar_binder)
            .await?;

        if let Some(selection) = &selection {
            if !self.check_allowed_scalar_expr_with_subquery(selection)? {
                return Err(ErrorCode::SemanticError(
                    "selection in delete statement can't contain window|aggregate|udf functions"
                        .to_string(),
                )
                .set_span(selection.span()));
            }
        }

        let plan = DeletePlan {
            catalog_name,
            database_name,
            table_name,
            metadata: self.metadata.clone(),
            selection,
            subquery_desc,
        };
        Ok(Plan::Delete(Box::new(plan)))
    }
}

impl Binder {
    // The method will find all subquery in filter
    fn has_subquery_in_selection(&self, scalar: &ScalarExpr) -> Result<bool> {
        struct FindSubqueryVisitor {
            found_subquery: bool,
        }

        impl<'a> Visitor<'a> for FindSubqueryVisitor {
            fn visit_subquery(&mut self, _subquery: &'a SubqueryExpr) -> Result<()> {
                self.found_subquery = true;
                Ok(())
            }
        }

        let mut find_subquery = FindSubqueryVisitor {
            found_subquery: false,
        };
        find_subquery.visit(scalar)?;

        Ok(find_subquery.found_subquery)
    }

    #[async_backtrace::framed]
    async fn process_subquery(
        &self,
        predicate: ScalarExpr,
        mut table_expr: SExpr,
    ) -> Result<SubqueryDesc> {
        let mut outer_columns: HashSet<usize> = Default::default();

        let filter = Filter {
            predicates: vec![predicate.clone()],
        };

        debug_assert_eq!(table_expr.plan.rel_op(), RelOp::Scan);
        let mut scan = match &*table_expr.plan {
            Scan(scan) => scan.clone(),
            _ => unreachable!(),
        };
        // Check if metadata contains row_id column
        let mut row_id_index = None;
        for col in self
            .metadata
            .read()
            .columns_by_table_index(scan.table_index)
            .iter()
        {
            if col.name() == ROW_ID_COL_NAME {
                row_id_index = Some(col.index());
                break;
            }
        }
        if row_id_index.is_none() {
            // Add row_id column to metadata
            let internal_column = INTERNAL_COLUMN_FACTORY
                .get_internal_column(ROW_ID_COL_NAME)
                .unwrap();
            row_id_index = Some(
                self.metadata
                    .write()
                    .add_internal_column(scan.table_index, internal_column),
            );
        }
        // Add row_id column to scan's column set
        scan.columns.insert(row_id_index.unwrap());
        table_expr.plan = Arc::new(Scan(scan.clone()));
        let filter_expr =
            SExpr::create_unary(Arc::new(filter.into()), Arc::new(table_expr.clone()));
        let mut rewriter = SubqueryRewriter::new(self.ctx.clone(), self.metadata.clone());
        let filter_expr = rewriter.rewrite(&filter_expr)?;

        let subquery_filter = match &*filter_expr.plan {
            RelOperatorFilter(filter) => filter.predicates[0].clone(),
            _ => unreachable!(),
        };

        // join the result
        // _row_id
        let row_id_column_binding = ColumnBinding {
            database_name: None,
            table_name: None,
            column_position: None,
            table_index: Some(scan.table_index),
            column_name: ROW_ID_COL_NAME.to_string(),
            index: row_id_index.unwrap(),
            data_type: Box::new(DataType::Number(NumberDataType::UInt64)),
            visibility: crate::Visibility::Visible,
            virtual_computed_expr: None,
        };
        let conditions = vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: row_id_column_binding.clone(),
        })];
        let join = Join {
            left_conditions: conditions.clone(),
            right_conditions: conditions,
            non_equi_conditions: vec![],
            join_type: crate::plans::JoinType::Left,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
        }
        .into();

        // use filter children as join right child, and add filter predicate result into outer_columns
        let input_expr = SExpr::create_binary(
            Arc::new(join),
            Arc::new(table_expr.clone()),
            Arc::new(filter_expr.children[0].as_ref().clone()),
        );
        let predicate_columns = if let RelOperator::Filter(filter) = filter_expr.plan.as_ref() {
            filter.used_columns()?
        } else {
            return Err(ErrorCode::from_string(
                "subquery data type in delete/update statement should be boolean".to_string(),
            ));
        };

        // add all table columns into outer columns
        let metadata = self.metadata.read();
        let columns = metadata.columns_by_table_index(scan.table_index);
        for column in columns {
            if let ColumnEntry::BaseTableColumn(column) = &column {
                outer_columns.insert(column.column_index);
            }
        }

        Ok(SubqueryDesc {
            input_expr,
            table_expr: filter_expr.children[0].as_ref().clone(),
            outer_columns,
            predicate_columns,
            index: row_id_index.unwrap(),
            filter: subquery_filter,
        })
    }
}
