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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator::Inner;
use databend_common_ast::ast::JoinOperator::RightAnti;
use databend_common_ast::ast::JoinOperator::RightOuter;
use databend_common_ast::ast::TableReference;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::TableSchema;
use databend_common_expression::ROW_ID_COL_NAME;

use crate::binder::util::TableIdentifier;
use crate::binder::Binder;
use crate::binder::DataMutationStrategy;
use crate::binder::DataMutationType;
use crate::binder::Finder;
use crate::binder::InternalColumnBinding;
use crate::optimizer::SExpr;
use crate::optimizer::SubqueryRewriter;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::MaterializedCte;
use crate::plans::MutationSource;
use crate::plans::RelOperator;
use crate::plans::SubqueryExpr;
use crate::plans::Visitor;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnSet;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;
use crate::DUMMY_COLUMN_INDEX;

pub enum DataMutationExpression {
    Merge {
        target: TableReference,
        source: TableReference,
        match_expr: Expr,
        has_star_clause: bool,
        mutation_strategy: DataMutationStrategy,
    },
    Update {
        target: TableReference,
        filter: Option<Expr>,
    },
    Delete {
        target: TableReference,
        filter: Option<Expr>,
    },
}

pub struct DataMutationExpressionBindResult {
    pub input: SExpr,
    pub mutation_type: DataMutationType,
    pub mutation_strategy: DataMutationStrategy,
    pub required_columns: ColumnSet,
    pub bind_context: BindContext,
    pub all_source_columns: Option<HashMap<usize, ScalarExpr>>,
    pub target_table_index: usize,
    pub target_row_id_index: usize,
    pub predicate_index: Option<usize>,
    pub truncate_table: bool,
    pub mutation_filter: Option<ScalarExpr>,
}

impl DataMutationExpression {
    pub async fn bind(
        &self,
        binder: &mut Binder,
        bind_context: &mut BindContext,
        target_table: Arc<dyn Table>,
        target_table_identifier: &TableIdentifier,
        target_table_schema: Arc<TableSchema>,
    ) -> Result<DataMutationExpressionBindResult> {
        let target_table_index = binder
            .metadata
            .read()
            .get_table_index(
                Some(target_table_identifier.database_name().as_str()),
                target_table_identifier.table_name().as_str(),
            )
            .ok_or_else(|| ErrorCode::Internal("Can't get target table index"))?;
        let mutation_type = self.data_mutation_type();
        let mut required_columns = ColumnSet::new();
        let mut update_stream_columns = target_table.change_tracking_enabled();

        match self {
            DataMutationExpression::Merge {
                target,
                source,
                match_expr,
                has_star_clause,
                mutation_strategy,
            } => {
                // Bind source table reference.
                let (mut source_s_expr, mut source_context) =
                    binder.bind_table_reference(bind_context, source)?;

                // Bind target table reference.
                let (mut target_s_expr, mut target_context) =
                    binder.bind_table_reference(bind_context, target)?;

                // Remove stream columns in source context.
                source_context
                    .columns
                    .retain(|v| v.visibility == Visibility::Visible);

                // Add source table columns to required columns.
                for column_index in source_context.column_set().iter() {
                    required_columns.insert(*column_index);
                }

                // Wrap `LogicalMaterializedCte` to `source_expr`.
                source_s_expr = binder.wrap_cte(source_s_expr);

                // When there is "update *" or "insert *", prepare all source columns.
                let all_source_columns = Self::all_source_columns(
                    *has_star_clause,
                    &source_context,
                    target_table_schema,
                )?;

                // TODO(Dousir9): do not add row_id column for insert only.
                // Add internal column _row_id for target_table.
                let target_row_id_index = binder.add_row_id_column(
                    &mut target_context,
                    target_table_identifier,
                    target_table_index,
                    &mut target_s_expr,
                    DataMutationType::Merge,
                )?;

                // Add target table row_id column to required columns.
                if *mutation_strategy != DataMutationStrategy::NotMatchedOnly {
                    required_columns.insert(target_row_id_index);
                }

                // If it is insert only, we don't need to update stream columns.
                if *mutation_strategy == DataMutationStrategy::NotMatchedOnly {
                    update_stream_columns = false;
                }
                let is_lazy_table = *mutation_strategy != DataMutationStrategy::NotMatchedOnly;
                target_s_expr =
                    update_target_scan(&target_s_expr, is_lazy_table, update_stream_columns)?;

                // Construct join, we use _row_id to check duplicate join rows.
                let (join_s_expr, bind_context) = binder
                    .bind_merge_into_join(
                        bind_context,
                        target_context.clone(),
                        source_context,
                        target_s_expr,
                        source_s_expr,
                        match mutation_strategy {
                            DataMutationStrategy::MatchedOnly => Inner,
                            DataMutationStrategy::NotMatchedOnly => RightAnti,
                            DataMutationStrategy::MixedMatched => RightOuter,
                            DataMutationStrategy::Direct => unreachable!(),
                        },
                        JoinCondition::On(Box::new(match_expr.clone())),
                    )
                    .await?;

                Ok(DataMutationExpressionBindResult {
                    input: join_s_expr,
                    mutation_type,
                    mutation_strategy: mutation_strategy.clone(),
                    required_columns,
                    bind_context,
                    all_source_columns,
                    target_table_index,
                    target_row_id_index,
                    predicate_index: None,
                    truncate_table: false,
                    mutation_filter: None,
                })
            }
            DataMutationExpression::Update { target, filter }
            | DataMutationExpression::Delete { target, filter } => {
                // Bind target table reference.
                let (mut s_expr, mut bind_context) =
                    binder.bind_table_reference(bind_context, target)?;

                let (mutation_strategy, filter) =
                    binder.process_filter(&mut bind_context, filter)?;

                let mut required_columns = ColumnSet::new();
                let mut target_row_id_index = DUMMY_COLUMN_INDEX;
                let mut predicate_index = None;
                let mut truncate_table = false;
                let s_expr = if mutation_strategy == DataMutationStrategy::Direct {
                    let mut read_partition_columns = HashSet::new();
                    if let Some(filter) = &filter {
                        read_partition_columns.extend(filter.used_columns());
                        if mutation_type == DataMutationType::Update {
                            let predicate_column_index =
                                binder.metadata.write().add_derived_column(
                                    "_predicate".to_string(),
                                    DataType::Boolean,
                                    None,
                                );
                            required_columns.insert(predicate_column_index);
                            predicate_index = Some(predicate_column_index);
                        }
                    } else if mutation_type == DataMutationType::Delete {
                        truncate_table = true;
                    }
                    let table_schema = target_table
                        .schema_with_stream()
                        .remove_virtual_computed_fields();
                    let target_mutation_source = MutationSource {
                        table_index: target_table_index,
                        schema: table_schema,
                        columns: bind_context.column_set(),
                        update_stream_columns,
                        filter: filter.clone(),
                        predicate_index,
                        input_type: mutation_type.clone(),
                        read_partition_columns,
                    };
                    for column_index in bind_context.column_set().iter() {
                        required_columns.insert(*column_index);
                    }
                    SExpr::create_leaf(Arc::new(RelOperator::MutationSource(
                        target_mutation_source,
                    )))
                } else {
                    let is_lazy_table = mutation_type != DataMutationType::Delete;
                    s_expr = update_target_scan(&s_expr, is_lazy_table, update_stream_columns)?;

                    // Add internal_column row_id for target_table
                    target_row_id_index = binder.add_row_id_column(
                        &mut bind_context,
                        target_table_identifier,
                        target_table_index,
                        &mut s_expr,
                        mutation_type.clone(),
                    )?;
                    // Add target table row_id column to required columns.
                    required_columns.insert(target_row_id_index);

                    let predicates = Binder::flatten_and_scalar_expr(filter.as_ref().unwrap());
                    let filter: Filter = Filter { predicates };
                    s_expr = SExpr::create_unary(Arc::new(filter.into()), Arc::new(s_expr));
                    let mut rewriter =
                        SubqueryRewriter::new(binder.ctx.clone(), binder.metadata.clone(), None);
                    rewriter.rewrite(&s_expr)?
                };

                Ok(DataMutationExpressionBindResult {
                    input: s_expr,
                    mutation_type,
                    mutation_strategy,
                    required_columns,
                    bind_context,
                    all_source_columns: None,
                    target_table_index,
                    target_row_id_index,
                    predicate_index,
                    truncate_table,
                    mutation_filter: filter,
                })
            }
        }
    }

    pub fn data_mutation_type(&self) -> DataMutationType {
        match self {
            DataMutationExpression::Merge { .. } => DataMutationType::Merge,
            DataMutationExpression::Update { .. } => DataMutationType::Update,
            DataMutationExpression::Delete { .. } => DataMutationType::Delete,
        }
    }

    // When there is "update *" or "insert *", prepare all source columns.
    pub fn all_source_columns(
        has_star_clause: bool,
        source_context: &BindContext,
        target_table_schema: Arc<TableSchema>,
    ) -> Result<Option<HashMap<usize, ScalarExpr>>> {
        if !has_star_clause {
            return Ok(None);
        }

        let default_target_table_schema = target_table_schema.remove_computed_fields();
        let mut all_columns = HashMap::with_capacity(default_target_table_schema.num_fields());
        // Use Vec as value, since there may be duplicate names.
        let mut name_map = HashMap::<String, Vec<ColumnBinding>>::new();
        for column in source_context.columns.iter() {
            name_map
                .entry(column.column_name.clone())
                .or_default()
                .push(column.clone());
        }

        for (field_idx, field) in default_target_table_schema.fields.iter().enumerate() {
            let column = match name_map.get(field.name()) {
                None => {
                    return Err(ErrorCode::SemanticError(
                        format!("can't find {} in source output", field.name).to_string(),
                    ));
                }
                Some(indices) => {
                    if indices.len() != 1 {
                        return Err(ErrorCode::SemanticError(
                            format!(
                                "there should be only one {} in source output,but we get {}",
                                field.name,
                                indices.len()
                            )
                            .to_string(),
                        ));
                    }

                    indices[0].clone()
                }
            };
            let column = ColumnBindingBuilder::new(
                field.name.to_string(),
                column.index,
                column.data_type.clone(),
                Visibility::Visible,
            )
            .build();
            let col = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });

            all_columns.insert(field_idx, col);
        }
        Ok(Some(all_columns))
    }
}

impl Binder {
    fn add_row_id_column(
        &mut self,
        bind_context: &mut BindContext,
        target_table_identifier: &TableIdentifier,
        table_index: usize,
        expr: &mut SExpr,
        mutation_type: DataMutationType,
    ) -> Result<usize> {
        let row_id_column_binding = InternalColumnBinding {
            database_name: Some(target_table_identifier.database_name().clone()),
            table_name: Some(target_table_identifier.table_name().clone()),
            internal_column: InternalColumn {
                column_name: ROW_ID_COL_NAME.to_string(),
                column_type: InternalColumnType::RowId,
            },
        };

        let column_binding = match bind_context.add_internal_column_binding(
            &row_id_column_binding,
            self.metadata.clone(),
            true,
        ) {
            Ok(column_binding) => column_binding,
            Err(_) => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Table {} does not support {}",
                    target_table_identifier.table_name(),
                    mutation_type,
                )));
            }
        };

        let row_id_index: usize = column_binding.index;

        *expr = SExpr::add_internal_column_index(expr, table_index, row_id_index, &None);

        self.metadata
            .write()
            .set_table_row_id_index(table_index, row_id_index);

        Ok(row_id_index)
    }

    fn wrap_cte(&mut self, mut s_expr: SExpr) -> SExpr {
        for (_, cte_info) in self.ctes_map.iter().rev() {
            if !cte_info.materialized || cte_info.used_count == 0 {
                continue;
            }
            let cte_s_expr = self.m_cte_bound_s_expr.get(&cte_info.cte_idx).unwrap();
            let left_output_columns = cte_info.columns.clone();
            s_expr = SExpr::create_binary(
                Arc::new(RelOperator::MaterializedCte(MaterializedCte {
                    left_output_columns,
                    cte_idx: cte_info.cte_idx,
                })),
                Arc::new(cte_s_expr.clone()),
                Arc::new(s_expr),
            );
        }
        s_expr
    }

    // Recursively flatten the AND expressions.
    pub fn flatten_and_scalar_expr(scalar: &ScalarExpr) -> Vec<ScalarExpr> {
        if let ScalarExpr::FunctionCall(func) = scalar
            && func.func_name == "and"
        {
            func.arguments
                .iter()
                .flat_map(Self::flatten_and_scalar_expr)
                .collect()
        } else {
            vec![scalar.clone()]
        }
    }

    pub(in crate::planner::binder) fn process_filter(
        &self,
        bind_context: &mut BindContext,
        filter: &Option<Expr>,
    ) -> Result<(DataMutationStrategy, Option<ScalarExpr>)> {
        if let Some(expr) = filter {
            let mut scalar_binder = ScalarBinder::new(
                bind_context,
                self.ctx.clone(),
                &self.name_resolution_ctx,
                self.metadata.clone(),
                &[],
                self.m_cte_bound_ctx.clone(),
                self.ctes_map.clone(),
            );
            let (scalar, _) = scalar_binder.bind(expr)?;
            if !self.check_allowed_scalar_expr_with_subquery(&scalar)? {
                return Err(ErrorCode::SemanticError(
                    "filter in mutation statement can't contain window|aggregate|udf functions"
                        .to_string(),
                )
                .set_span(scalar.span()));
            }
            if !self.has_subquery(&scalar)? {
                Ok((DataMutationStrategy::Direct, Some(scalar)))
            } else {
                Ok((DataMutationStrategy::MatchedOnly, Some(scalar)))
            }
        } else {
            Ok((DataMutationStrategy::Direct, None))
        }
    }

    fn has_subquery(&self, scalar: &ScalarExpr) -> Result<bool> {
        struct SubqueryVisitor {
            found_subquery: bool,
        }

        impl<'a> Visitor<'a> for SubqueryVisitor {
            fn visit_subquery(&mut self, _: &'a SubqueryExpr) -> Result<()> {
                self.found_subquery = true;
                Ok(())
            }
        }

        let mut subquery_visitor = SubqueryVisitor {
            found_subquery: false,
        };
        subquery_visitor.visit(scalar)?;

        Ok(subquery_visitor.found_subquery)
    }

    pub(crate) fn check_allowed_scalar_expr_with_subquery(
        &self,
        scalar: &ScalarExpr,
    ) -> Result<bool> {
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::AsyncFunctionCall(_)
                    | ScalarExpr::UDFCall(_)
            )
        };

        let mut finder = Finder::new(&f);
        finder.visit(scalar)?;
        Ok(finder.scalars().is_empty())
    }
}

pub fn update_target_scan(
    s_expr: &SExpr,
    is_lazy_table: bool,
    update_stream_columns: bool,
) -> Result<SExpr> {
    if !is_lazy_table && !update_stream_columns {
        return Ok(s_expr.clone());
    }
    match s_expr.plan() {
        RelOperator::Scan(scan) => {
            let mut scan = scan.clone();
            scan.is_lazy_table = is_lazy_table;
            scan.set_update_stream_columns(update_stream_columns);
            Ok(SExpr::create_leaf(Arc::new(scan.into())))
        }
        _ => {
            let mut children = Vec::with_capacity(s_expr.arity());
            for child in s_expr.children() {
                let child = update_target_scan(child, is_lazy_table, update_stream_columns)?;
                children.push(Arc::new(child));
            }
            Ok(s_expr.replace_children(children))
        }
    }
}

pub fn target_table_position(s_expr: &SExpr, target_table_index: usize) -> Result<usize> {
    if !matches!(s_expr.plan(), RelOperator::Join(_)) {
        return Ok(0);
    }
    fn contains_target_table(s_expr: &SExpr, target_table_index: usize) -> bool {
        if let RelOperator::Scan(ref scan) = s_expr.plan() {
            scan.table_index == target_table_index
        } else {
            s_expr
                .children()
                .any(|child| contains_target_table(child, target_table_index))
        }
    }

    if contains_target_table(s_expr.child(0)?, target_table_index) {
        Ok(0)
    } else {
        Ok(1)
    }
}
