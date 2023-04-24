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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use common_base::runtime::Thread;
use common_catalog::table_context::TableContext;
use common_exception::Result;

use crate::optimizer::hyper_dp::join_node::JoinNode;
use crate::optimizer::hyper_dp::join_relation::JoinRelation;
use crate::optimizer::hyper_dp::join_relation::RelationSetTree;
use crate::optimizer::hyper_dp::query_graph::QueryGraph;
use crate::optimizer::hyper_dp::util::intersect;
use crate::optimizer::hyper_dp::util::union;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleFactory;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::RelOperator;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

const MOCK_NUMBER: usize = 1000;
const RELATION_THRESHOLD: usize = 10;

// The join reorder algorithm follows the paper: Dynamic Programming Strikes Back
// See the paper for more details.
// Current, we only support inner join
pub struct DPhpy {
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    join_relations: Vec<JoinRelation>,
    // base table index -> index of join_relations
    table_index_map: HashMap<IndexType, IndexType>,
    dp_table: HashMap<Vec<IndexType>, JoinNode>,
    subquery_table_index_map: HashMap<IndexType, HashMap<IndexType, IndexType>>,
    query_graph: QueryGraph,
    relation_set_tree: RelationSetTree,
    filters: HashSet<Filter>,
}

impl DPhpy {
    pub fn new(ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            ctx,
            metadata,
            join_relations: vec![],
            table_index_map: Default::default(),
            dp_table: Default::default(),
            subquery_table_index_map: Default::default(),
            query_graph: QueryGraph::new(),
            relation_set_tree: Default::default(),
            filters: HashSet::new(),
        }
    }

    // Traverse the s_expr and get all base relations and join conditions
    fn get_base_relations(
        &mut self,
        s_expr: &SExpr,
        join_conditions: &mut Vec<(ScalarExpr, ScalarExpr)>,
        join_child: bool,
        is_subquery: bool,
        join_relation: Option<SExpr>,
    ) -> Result<bool> {
        // Start to traverse and stop when meet join
        if s_expr.is_pattern() {
            return Ok(false);
        }

        if is_subquery {
            // If it's a subquery, start a new dphyp
            let mut dphyp = DPhpy::new(self.ctx.clone(), self.metadata.clone());
            let (res, optimized) = dphyp.optimize(s_expr.clone())?;
            if optimized {
                let key = self.subquery_table_index_map.len() + MOCK_NUMBER;
                self.subquery_table_index_map
                    .insert(key, dphyp.table_index_map);
                self.table_index_map
                    .insert(key, self.join_relations.len() as IndexType);
                self.join_relations.push(JoinRelation::new(&res));
            }
            return Ok(optimized);
        }

        match &s_expr.plan {
            RelOperator::Scan(op) => {
                let join_relation = if let Some(relation) = join_relation {
                    // Check if relation contains filter, if exists, check if the filter in `filters`
                    // If exists, remove it from `filters`
                    self.check_filter(&relation);
                    JoinRelation::new(&relation)
                } else {
                    JoinRelation::new(s_expr)
                };
                self.table_index_map
                    .insert(op.table_index, self.join_relations.len() as IndexType);
                self.join_relations.push(join_relation);
                Ok(true)
            }
            RelOperator::Join(op) => {
                if !matches!(op.join_type, JoinType::Inner | JoinType::Cross) {
                    return Ok(false);
                }
                let mut left_is_subquery = false;
                let mut right_is_subquery = false;
                // Fixme: If join's child is EvalScalar, we think it is a subquery.
                // Check join's child is filter or scan
                let left_op = &s_expr.children()[0].plan;
                let right_op = &s_expr.children()[1].plan;
                if matches!(left_op, RelOperator::EvalScalar(_)) {
                    left_is_subquery = true;
                }
                if matches!(right_op, RelOperator::EvalScalar(_)) {
                    right_is_subquery = true;
                }
                // Add join conditions
                for condition_pair in op.left_conditions.iter().zip(op.right_conditions.iter()) {
                    join_conditions.push((condition_pair.0.clone(), condition_pair.1.clone()));
                }
                if !op.non_equi_conditions.is_empty() {
                    let filter = Filter {
                        predicates: op.non_equi_conditions.clone(),
                        is_having: false,
                    };
                    self.filters.insert(filter);
                }
                if left_is_subquery && right_is_subquery {
                    // Parallel process subquery
                    let ctx = self.ctx.clone();
                    let metadata = self.metadata.clone();
                    let left_expr = s_expr.children()[0].clone();
                    let left_res = Thread::spawn(move || {
                        let mut dphyp = DPhpy::new(ctx, metadata);
                        (dphyp.optimize(left_expr), dphyp.table_index_map)
                    });
                    let ctx = self.ctx.clone();
                    let metadata = self.metadata.clone();
                    let right_expr = s_expr.children()[1].clone();
                    let right_res = Thread::spawn(move || {
                        let mut dphyp = DPhpy::new(ctx, metadata);
                        (dphyp.optimize(right_expr), dphyp.table_index_map)
                    });
                    let left_res = left_res.join()?;
                    let (left_expr, left_optimized) = left_res.0?;
                    let right_res = right_res.join()?;
                    let (right_expr, right_optimized) = right_res.0?;
                    if left_optimized && right_optimized {
                        let key = self.subquery_table_index_map.len() + MOCK_NUMBER;
                        self.subquery_table_index_map.insert(key, left_res.1);
                        self.table_index_map
                            .insert(key, self.join_relations.len() as IndexType);
                        self.join_relations.push(JoinRelation::new(&left_expr));
                        let key = self.subquery_table_index_map.len() + MOCK_NUMBER;
                        self.subquery_table_index_map.insert(key, right_res.1);
                        self.table_index_map
                            .insert(key, self.join_relations.len() as IndexType);
                        self.join_relations.push(JoinRelation::new(&right_expr));
                        Ok(true)
                    } else {
                        Ok(false)
                    }
                } else {
                    let left_res = self.get_base_relations(
                        &s_expr.children()[0],
                        join_conditions,
                        true,
                        left_is_subquery,
                        None,
                    )?;
                    let right_res = self.get_base_relations(
                        &s_expr.children()[1],
                        join_conditions,
                        true,
                        right_is_subquery,
                        None,
                    )?;
                    Ok(left_res && right_res)
                }
            }

            RelOperator::ProjectSet(_)
            | RelOperator::EvalScalar(_)
            | RelOperator::Filter(_)
            | RelOperator::Aggregate(_)
            | RelOperator::Sort(_)
            | RelOperator::Limit(_) => {
                if join_child {
                    // If plan in filter, save it
                    if let RelOperator::Filter(op) = &s_expr.plan {
                        self.filters.insert(op.clone());
                    }
                    self.get_base_relations(
                        &s_expr.children()[0],
                        join_conditions,
                        true,
                        false,
                        Some(s_expr.clone()),
                    )
                } else {
                    self.get_base_relations(
                        &s_expr.children()[0],
                        join_conditions,
                        false,
                        false,
                        None,
                    )
                }
            }
            RelOperator::Exchange(_) | RelOperator::Pattern(_) => unreachable!(),
            RelOperator::Window(_)
            | RelOperator::UnionAll(_)
            | RelOperator::DummyTableScan(_)
            | RelOperator::RuntimeFilterSource(_) => Ok(false),
        }
    }

    // The input plan tree has been optimized by heuristic optimizer
    // So filters have pushed down join and cross join has been converted to inner join as possible as we can
    // The output plan will have optimal join order theoretically
    pub fn optimize(&mut self, s_expr: SExpr) -> Result<(SExpr, bool)> {
        // Firstly, we need to extract all join conditions and base tables
        // `join_condition` is pair, left is left_condition, right is right_condition
        let mut join_conditions = vec![];
        let res = self.get_base_relations(&s_expr, &mut join_conditions, false, false, None)?;
        if !res {
            return Ok((s_expr, false));
        }
        if self.join_relations.len() == 1 {
            return Ok((s_expr, true));
        }

        // Second, use `join_conditions` to create edges in `query_graph`
        for (left_condition, right_condition) in join_conditions.iter() {
            // Find the corresponding join relations in `join_conditions`
            let mut left_relation_set = HashSet::new();
            let mut right_relation_set = HashSet::new();
            let left_used_tables = left_condition.used_tables(self.metadata.clone())?;
            for table in left_used_tables.iter() {
                if let Some(idx) = self.table_index_map.get(table) {
                    left_relation_set.insert(*idx);
                }
                // Start to traverse `subquery_table_index_map` to find the corresponding table index
                for (key, val) in self.subquery_table_index_map.iter() {
                    if val.get(table).is_some() {
                        if let Some(idx) = self.table_index_map.get(key) {
                            left_relation_set.insert(*idx);
                        }
                    }
                }
            }
            let right_used_tables = right_condition.used_tables(self.metadata.clone())?;
            for table in right_used_tables.iter() {
                if let Some(idx) = self.table_index_map.get(table) {
                    right_relation_set.insert(*idx);
                }
                for (key, val) in self.subquery_table_index_map.iter() {
                    if val.get(table).is_some() {
                        if let Some(idx) = self.table_index_map.get(key) {
                            right_relation_set.insert(*idx);
                        }
                    }
                }
            }

            if !left_relation_set.is_empty() && !right_relation_set.is_empty() {
                // Use `left_relation_set` and `right_relation_set` to initial RelationSetTree
                let left_relation_set = self
                    .relation_set_tree
                    .get_relation_set(&left_relation_set)?;
                let right_relation_set = self
                    .relation_set_tree
                    .get_relation_set(&right_relation_set)?;
                // Try to create edges if `left_relation_set` and `right_relation_set` are not disjoint
                if !intersect(&left_relation_set, &right_relation_set) {
                    self.query_graph.create_edges(
                        &left_relation_set,
                        &right_relation_set,
                        (left_condition.clone(), right_condition.clone()),
                    )?;
                    self.query_graph.create_edges(
                        &right_relation_set,
                        &left_relation_set,
                        (right_condition.clone(), left_condition.clone()),
                    )?;
                }
                continue;
            }
        }
        let optimized = self.solve()?;
        // Get all join relations in `relation_set_tree`
        let all_relations = self
            .relation_set_tree
            .get_relation_set(&(0..self.join_relations.len()).collect())?;
        if optimized {
            if let Some(final_plan) = self.dp_table.get(&all_relations) {
                self.join_reorder(final_plan, s_expr)
            } else {
                // Maybe exist cross join, which make graph disconnected
                Ok((s_expr.clone(), false))
            }
        } else {
            Ok((s_expr.clone(), false))
        }
    }

    // This method will run dynamic programming algorithm to find the optimal join order
    fn solve(&mut self) -> Result<bool> {
        // Initial `dp_table` with plan for single relation
        for (idx, relation) in self.join_relations.iter().enumerate() {
            // Get nodes  in `relation_set_tree`
            let nodes = self.relation_set_tree.get_relation_set_by_index(idx)?;
            let ce = relation.cardinality()?;
            let join = JoinNode {
                join_type: JoinType::Inner,
                leaves: Arc::new(nodes.clone()),
                children: Arc::new(vec![]),
                join_conditions: Arc::new(vec![]),
                cost: 0.0,
                cardinality: Some(ce),
                s_expr: None,
            };
            let _ = self.dp_table.insert(nodes, join);
        }

        // Choose all nodes as enumeration start node once (desc order)
        for idx in (0..self.join_relations.len()).rev() {
            // Get node from `relation_set_tree`
            let node = self.relation_set_tree.get_relation_set_by_index(idx)?;
            // Emit node as subgraph
            if !self.emit_csg(&node)? {
                return Ok(false);
            }

            // Create forbidden node set
            // Forbid node idx will less than current idx
            let forbidden_nodes = (0..idx).collect();
            // Enlarge the subgraph recursively
            if !self.enumerate_csg_rec(&node, &forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EmitCsg will take a non-empty subset of hyper_graph's nodes(V) which contains a connected subgraph.
    // Then it will possibly generate a connected complement which will combine `nodes` to be a csg-cmp-pair.
    fn emit_csg(&mut self, nodes: &[IndexType]) -> Result<bool> {
        if nodes.len() == self.join_relations.len() {
            return Ok(true);
        }
        let mut forbidden_nodes: HashSet<IndexType> = (0..(nodes[0])).collect();
        forbidden_nodes.extend(nodes);

        // Get neighbors of `nodes`
        let mut neighbors = self.query_graph.neighbors(nodes, &forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }
        neighbors.sort();
        // Traverse all neighbors by desc
        for neighbor in neighbors.iter().rev() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;
            // Check if neighbor is connected with `nodes`
            let join_conditions = self.query_graph.is_connected(nodes, &neighbor_relations)?;
            if !join_conditions.is_empty()
                && !self.emit_csg_cmp(nodes, &neighbor_relations, join_conditions)?
            {
                return Ok(false);
            }
            if !self.enumerate_cmp_rec(nodes, &neighbor_relations, &forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EnumerateCsgRec will extend the given `nodes`.
    // It'll consider each non-empty, proper subset of the neighborhood of nodes that are not forbidden.
    fn enumerate_csg_rec(
        &mut self,
        nodes: &[IndexType],
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<bool> {
        let mut neighbors = self.query_graph.neighbors(nodes, forbidden_nodes)?;
        if neighbors.is_empty() {
            return Ok(true);
        }
        if self.join_relations.len() >= RELATION_THRESHOLD {
            // Only consider the nodes.len() neighbors to reduce search space
            neighbors = neighbors
                .iter()
                .take(nodes.len())
                .copied()
                .collect::<Vec<IndexType>>();
        }
        let mut merged_sets = Vec::new();
        for neighbor in neighbors.iter() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;
            let merged_relation_set = union(nodes, &neighbor_relations);
            if self.dp_table.contains_key(&merged_relation_set)
                && merged_relation_set.len() > nodes.len()
                && !self.emit_csg(&merged_relation_set)?
            {
                return Ok(false);
            }
            merged_sets.push(merged_relation_set);
        }

        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (idx, neighbor) in neighbors.iter().enumerate() {
            if self.join_relations.len() < RELATION_THRESHOLD {
                new_forbidden_nodes = forbidden_nodes.clone();
            }
            new_forbidden_nodes.insert(*neighbor);
            if !self.enumerate_csg_rec(&merged_sets[idx], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // EmitCsgCmp will join the optimal plan from left and right
    fn emit_csg_cmp(
        &mut self,
        left: &[IndexType],
        right: &[IndexType],
        mut join_conditions: Vec<(ScalarExpr, ScalarExpr)>,
    ) -> Result<bool> {
        debug_assert!(self.dp_table.contains_key(left));
        debug_assert!(self.dp_table.contains_key(right));
        let parent_set = union(left, right);
        let mut left_join = self.dp_table.get(left).unwrap().clone();
        let mut right_join = self.dp_table.get(right).unwrap().clone();
        let left_cardinality = left_join.cardinality(&self.join_relations)?;
        let right_cardinality = right_join.cardinality(&self.join_relations)?;

        if left_cardinality < right_cardinality {
            for join_condition in join_conditions.iter_mut() {
                std::mem::swap(&mut join_condition.0, &mut join_condition.1);
            }
        }
        let parent_node = self.dp_table.get(&parent_set);
        let mut join_node = if !join_conditions.is_empty() {
            JoinNode {
                join_type: JoinType::Inner,
                leaves: Arc::new(parent_set.clone()),
                children: if left_cardinality < right_cardinality {
                    Arc::new(vec![right_join, left_join])
                } else {
                    Arc::new(vec![left_join, right_join])
                },
                cost: 0.0,
                join_conditions: Arc::new(join_conditions),
                cardinality: None,
                s_expr: None,
            }
        } else {
            JoinNode {
                join_type: JoinType::Cross,
                leaves: Arc::new(parent_set.clone()),
                children: Arc::new(vec![left_join, right_join]),
                cost: left_cardinality * right_cardinality,
                join_conditions: Arc::new(vec![]),
                cardinality: None,
                s_expr: None,
            }
        };
        if join_node.join_type == JoinType::Inner {
            let cost = join_node.cardinality(&self.join_relations)?
                + join_node.children[0].cost
                + join_node.children[1].cost;
            join_node.set_cost(cost);
        }

        if parent_node.is_none() || parent_node.unwrap().cost > join_node.cost {
            // Update `dp_table`
            self.dp_table.insert(parent_set, join_node);
        }
        Ok(true)
    }

    // The second parameter is a set which is connected and must be extended until a valid csg-cmp-pair is reached.
    // Therefore, it considers the neighborhood of right.
    fn enumerate_cmp_rec(
        &mut self,
        left: &[IndexType],
        right: &[IndexType],
        forbidden_nodes: &HashSet<IndexType>,
    ) -> Result<bool> {
        let neighbor_set = self.query_graph.neighbors(right, forbidden_nodes)?;
        if neighbor_set.is_empty() {
            return Ok(true);
        }
        let mut merged_sets = Vec::new();
        for neighbor in neighbor_set.iter() {
            let neighbor_relations = self
                .relation_set_tree
                .get_relation_set_by_index(*neighbor)?;
            // Merge `right` with `neighbor_relations`
            let merged_relation_set = union(right, &neighbor_relations);
            let join_conditions = self.query_graph.is_connected(left, &merged_relation_set)?;
            if merged_relation_set.len() > right.len()
                && self.dp_table.contains_key(&merged_relation_set)
                && !join_conditions.is_empty()
                && !self.emit_csg_cmp(left, &merged_relation_set, join_conditions)?
            {
                return Ok(false);
            }
            merged_sets.push(merged_relation_set);
        }
        // Continue to enumerate cmp
        let mut new_forbidden_nodes = forbidden_nodes.clone();
        for (idx, neighbor) in neighbor_set.iter().enumerate() {
            new_forbidden_nodes.insert(*neighbor);
            if !self.enumerate_cmp_rec(left, &merged_sets[idx], &new_forbidden_nodes)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    // Map join order in `JoinNode` to `SExpr`
    fn join_reorder(&self, final_plan: &JoinNode, s_expr: SExpr) -> Result<(SExpr, bool)> {
        // Convert `final_plan` to `SExpr`
        let join_expr = final_plan.s_expr(&self.join_relations);
        // Find first join node in `s_expr`, then replace it with `join_expr`
        let new_s_expr = self.replace_join_expr(&join_expr, &s_expr)?;
        Ok((new_s_expr, true))
    }

    #[allow(clippy::only_used_in_recursion)]
    fn replace_join_expr(&self, join_expr: &SExpr, s_expr: &SExpr) -> Result<SExpr> {
        let mut new_s_expr = s_expr.clone();
        match &s_expr.plan {
            RelOperator::Join(_) => {
                new_s_expr.plan = join_expr.plan.clone();
                new_s_expr.children = join_expr.children.clone();
                // Add filters to `new_s_expr`, then push down filters if possible
                let mut predicates = vec![];
                for filter in self.filters.iter() {
                    predicates.extend(filter.clone().predicates.iter().cloned())
                }
                new_s_expr = SExpr::create_unary(
                    RelOperator::Filter(Filter {
                        predicates,
                        is_having: false,
                    }),
                    new_s_expr,
                );
                new_s_expr = self.push_down_filter(&new_s_expr)?;
                if let RelOperator::Filter(filter) = &new_s_expr.plan {
                    if filter.predicates.is_empty() {
                        new_s_expr = new_s_expr.children[0].clone();
                    }
                }
            }
            _ => {
                let child_expr = self.replace_join_expr(join_expr, &s_expr.children[0])?;
                new_s_expr.children = Arc::new(vec![child_expr]);
            }
        }
        Ok(new_s_expr)
    }

    fn check_filter(&mut self, expr: &SExpr) {
        if let RelOperator::Filter(filter) = &expr.plan {
            if self.filters.contains(filter) {
                self.filters.remove(filter);
            }
        }
        for child in expr.children() {
            self.check_filter(child);
        }
    }

    fn push_down_filter(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(self.push_down_filter(expr)?);
        }
        let optimized_expr = s_expr.replace_children(optimized_children);
        let result = self.apply_rule(&optimized_expr)?;

        Ok(result)
    }

    fn apply_rule(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        let rule = RuleFactory::create_rule(
            RuleID::PushDownFilterJoin,
            self.metadata.clone(),
            self.ctx.get_function_context()?,
        )?;
        let mut state = TransformResult::new();
        if s_expr.match_pattern(&rule.patterns()[0]) && !s_expr.applied_rule(&rule.id()) {
            s_expr.set_applied_rule(&rule.id());
            rule.apply(&s_expr, &mut state)?;
            if !state.results().is_empty() {
                // Recursive optimize the result
                let result = &state.results()[0];
                let optimized_result = self.push_down_filter(result)?;
                return Ok(optimized_result);
            }
        }
        Ok(s_expr.clone())
    }
}
