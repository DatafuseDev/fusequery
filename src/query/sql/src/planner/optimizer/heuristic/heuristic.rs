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

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use once_cell::sync::Lazy;

use super::prune_unused_columns::UnusedColumnPruner;
use crate::optimizer::heuristic::decorrelate::decorrelate_subquery;
use crate::optimizer::heuristic::prewhere_optimization::PrewhereOptimizer;
use crate::optimizer::heuristic::RuleList;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::ColumnSet;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::BindContext;
use crate::MetadataRef;

pub static DEFAULT_REWRITE_RULES: Lazy<Vec<RuleID>> = Lazy::new(|| {
    vec![
        RuleID::NormalizeDisjunctiveFilter,
        RuleID::NormalizeScalarFilter,
        RuleID::EliminateFilter,
        RuleID::EliminateEvalScalar,
        RuleID::MergeFilter,
        RuleID::MergeEvalScalar,
        RuleID::PushDownFilterUnion,
        RuleID::PushDownLimitUnion,
        RuleID::PushDownLimitSort,
        RuleID::PushDownLimitOuterJoin,
        RuleID::PushDownLimitScan,
        RuleID::PushDownSortScan,
        RuleID::PushDownFilterEvalScalar,
        RuleID::PushDownFilterJoin,
        RuleID::FoldCountAggregate,
        RuleID::SplitAggregate,
        RuleID::PushDownFilterScan,
    ]
});

/// A heuristic query optimizer. It will apply specific transformation rules in order and
/// implement the logical plans with default implementation rules.
pub struct HeuristicOptimizer {
    rules: RuleList,

    _ctx: Arc<dyn TableContext>,
    bind_context: Box<BindContext>,
    metadata: MetadataRef,
}

impl HeuristicOptimizer {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        bind_context: Box<BindContext>,
        metadata: MetadataRef,
        rules: RuleList,
    ) -> Self {
        HeuristicOptimizer {
            rules,

            _ctx: ctx,
            bind_context,
            metadata,
        }
    }

    fn pre_optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let result = decorrelate_subquery(self.metadata.clone(), s_expr)?;
        Ok(result)
    }

    fn post_optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let prewhere_optimizer = PrewhereOptimizer::new(self.metadata.clone());
        let s_expr = prewhere_optimizer.prewhere_optimize(s_expr)?;

        let pruner = UnusedColumnPruner::new(self.metadata.clone());
        let require_columns: ColumnSet =
            self.bind_context.columns.iter().map(|c| c.index).collect();
        pruner.remove_unused_columns(&s_expr, require_columns)
    }

    pub fn optimize(&mut self, s_expr: SExpr) -> Result<SExpr> {
        let pre_optimized = self.pre_optimize(s_expr)?;
        let optimized = self.optimize_expression(&pre_optimized)?;
        let post_optimized = self.post_optimize(optimized)?;
        Ok(post_optimized)
    }

    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        /// manually implement a stack to avoid stack overflow
        /// an AST frame is a node in the AST tree
        struct TreeFrame {
            pub expr: Box<SExpr>,
            pub optimized_children: Vec<SExpr>,
            pub parent: Option<Rc<RefCell<TreeFrame>>>,
            pub visited: bool,
        }

        let mut to_optimize = VecDeque::new();
        let root_frame = TreeFrame {
            expr: Box::new(s_expr.clone()),
            optimized_children: Vec::new(),
            parent: None,
            visited: false,
        };
        let root_frame = Rc::new(RefCell::new(root_frame));
        to_optimize.push_back(root_frame);
        while let Some(frame) = to_optimize.pop_back() {
            let visited = { frame.borrow().visited };
            match visited {
                false => {
                    to_optimize.push_back(frame.clone());
                    // all of its children are not optimized
                    // Note:
                    // optimize starts from the first children!
                    for expr in frame.borrow().expr.children().iter().rev() {
                        let child_frame = TreeFrame {
                            expr: Box::new(expr.clone()),
                            optimized_children: Vec::new(),
                            parent: Some(frame.clone()),
                            visited: false,
                        };
                        let child_frame = Rc::new(RefCell::new(child_frame));
                        to_optimize.push_back(child_frame);
                    }
                    frame.borrow_mut().visited = true;
                }
                true => {
                    // all of its children are optimized now, apply transform rules on it
                    let (result, need_optimize) = {
                        let expr = &frame.borrow().expr;
                        let optimized_children = frame.borrow().optimized_children.clone();
                        let optimized_expr = expr.replace_children(optimized_children);
                        self.apply_transform_rules(&optimized_expr, &self.rules)?
                    };

                    if need_optimize {
                        // rule is applied and the expression needs optimize again
                        to_optimize.push_back(frame.clone());
                        let mut frame = frame.borrow_mut();
                        // move `result` onto heap
                        frame.expr = Box::new(result);
                        frame.optimized_children = vec![];
                        frame.visited = false;
                    } else {
                        // the result is optimized, push it up to the parent
                        match &frame.borrow().parent {
                            None => {
                                // is root node
                                // return the result
                                return Ok(result);
                            }
                            Some(parent) => {
                                // not root node
                                // push to parent
                                let mut parent = parent.borrow_mut();
                                parent.optimized_children.push(result);
                            }
                        }
                    }
                }
            }
        }
        unreachable!()
    }

    // Return Ok((expr, false)) if no rules matched
    //
    // Note:
    // this function will apply transform rules only
    //
    // if the expression still needs optimization
    // set the last bool to true
    fn apply_transform_rules(&self, s_expr: &SExpr, rule_list: &RuleList) -> Result<(SExpr, bool)> {
        let mut s_expr = s_expr.clone();
        println!("==apply transform rules==");
        for rule in rule_list.iter() {
            let mut state = TransformResult::new();
            if s_expr.match_pattern(rule.pattern()) && !s_expr.applied_rule(&rule.id()) {
                println!("rule matched: {:?}", rule.id());
                s_expr.apply_rule(&rule.id());
                rule.apply(&s_expr, &mut state)?;
                if !state.results().is_empty() {
                    let result = &state.results()[0];
                    println!("rule applied outcome: {:#?}", result.applied_rules);
                    // set to true
                    // the optimizer will optimize it again
                    return Ok((result.clone(), true));
                }
            }
        }

        Ok((s_expr.clone(), false))
    }
}
