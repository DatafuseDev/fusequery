// Copyright 2021 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::FunctionFactory;
use common_planners::AggregatorFinalPlan;
use common_planners::AggregatorPartialPlan;
use common_planners::Expression;
use common_planners::Expressions;
use common_planners::PlanBuilder;
use common_planners::PlanNode;
use common_planners::PlanRewriter;

use crate::optimizers::Optimizer;
use crate::sessions::DatabendQueryContextRef;

pub struct ExprTransformOptimizer {}

struct ExprTransformImpl {
    before_group_by_schema: Option<DataSchemaRef>,
}

impl ExprTransformImpl {
    fn inverse_expr<F>(
        op: &str,
        args: Expressions,
        origin: &Expression,
        is_negated: bool,
        f: F,
    ) -> Result<Expression>
    where
        F: Fn(&str, Expressions) -> Expression,
    {
        if !is_negated {
            return Ok(origin.clone());
        }

        let factory = FunctionFactory::instance();
        let function_features = factory.get_features(op)?;

        match function_features.negative_function_name.as_ref() {
            Some(v) => Ok(f(v, args)),
            None => Ok(Expression::create_unary_expression("NOT", vec![
                origin.clone()
            ])),
        }
    }

    fn truth_transformer(origin: &Expression, is_negated: bool) -> Result<Expression> {
        match origin {
            // TODO: support in and not in.
            Expression::BinaryExpression { op, left, right } => match op.to_lowercase().as_str() {
                "and" => {
                    let new_left = Self::truth_transformer(left, is_negated)?;
                    let new_right = Self::truth_transformer(right, is_negated)?;
                    if is_negated {
                        Ok(new_left.or(new_right))
                    } else {
                        Ok(new_left.and(new_right))
                    }
                }
                "or" => {
                    let new_left = Self::truth_transformer(left, is_negated)?;
                    let new_right = Self::truth_transformer(right, is_negated)?;
                    if is_negated {
                        Ok(new_left.and(new_right))
                    } else {
                        Ok(new_left.or(new_right))
                    }
                }
                other => Self::inverse_expr(
                    other,
                    vec![left.as_ref().clone(), right.as_ref().clone()],
                    origin,
                    is_negated,
                    Expression::create_binary_expression,
                ),
            },
            Expression::ScalarFunction { op, args } => Self::inverse_expr(
                op.to_lowercase().as_str(),
                args.clone(),
                origin,
                is_negated,
                Expression::create_scalar_function,
            ),
            Expression::UnaryExpression { op, expr } if op.to_lowercase().eq("not") => {
                Self::truth_transformer(expr, !is_negated)
            }
            _ => {
                if !is_negated {
                    Ok(origin.clone())
                } else {
                    Ok(Expression::create_unary_expression("NOT", vec![
                        origin.clone()
                    ]))
                }
            }
        }
    }
}

impl PlanRewriter for ExprTransformImpl {
    fn rewrite_expr(&mut self, _schema: &DataSchemaRef, expr: &Expression) -> Result<Expression> {
        Self::truth_transformer(expr, false)
    }

    fn rewrite_aggregate_partial(&mut self, plan: &AggregatorPartialPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;
        match self.before_group_by_schema {
            Some(_) => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be None",
            )),
            None => {
                self.before_group_by_schema = Some(new_input.schema());
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_partial(&new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }

    fn rewrite_aggregate_final(&mut self, plan: &AggregatorFinalPlan) -> Result<PlanNode> {
        let new_input = self.rewrite_plan_node(&plan.input)?;

        match self.before_group_by_schema.take() {
            None => Err(ErrorCode::LogicalError(
                "Logical error: before group by schema must be Some",
            )),
            Some(schema_before_group_by) => {
                let new_aggr_expr = self.rewrite_exprs(&new_input.schema(), &plan.aggr_expr)?;
                let new_group_expr = self.rewrite_exprs(&new_input.schema(), &plan.group_expr)?;
                PlanBuilder::from(&new_input)
                    .aggregate_final(schema_before_group_by, &new_aggr_expr, &new_group_expr)?
                    .build()
            }
        }
    }
}

impl ExprTransformImpl {
    pub fn new() -> ExprTransformImpl {
        ExprTransformImpl {
            before_group_by_schema: None,
        }
    }
}

impl Optimizer for ExprTransformOptimizer {
    fn name(&self) -> &str {
        "ExprTransform"
    }

    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut visitor = ExprTransformImpl::new();
        visitor.rewrite_plan_node(plan)
    }
}

impl ExprTransformOptimizer {
    pub fn create(_ctx: DatabendQueryContextRef) -> Self {
        ExprTransformOptimizer {}
    }
}
