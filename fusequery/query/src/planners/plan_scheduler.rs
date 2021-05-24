// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::min;

use common_exception::Result;
use common_planners::PlanNode;
use common_planners::PlanRewriter;
use common_planners::ReadDataSourcePlan;
use log::info;

use crate::sessions::FuseQueryContextRef;

pub struct PlanScheduler;

/// ReadSourceRewriter will replace all ReadDataSourcePlan in a plan tree with given `new_source_plan`
struct ReadSourceRewriter {
    new_source_plan: ReadDataSourcePlan,
}

impl<'plan> PlanRewriter<'plan> for ReadSourceRewriter {
    fn rewrite_read_data_source(&mut self, _plan: &ReadDataSourcePlan) -> Result<PlanNode> {
        Ok(PlanNode::ReadSource(self.new_source_plan.clone()))
    }
}

impl ReadSourceRewriter {
    fn rewrite(mut self, plan: &PlanNode) -> Result<PlanNode> {
        self.rewrite_plan_node(plan)
    }
}

impl PlanScheduler {
    /// Schedule the plan to Local or Remote mode.
    pub fn reschedule(
        ctx: FuseQueryContextRef,
        plan: &PlanNode,
    ) -> Result<Vec<(String, PlanNode)>> {
        let mut results = vec![];
        let max_threads = ctx.get_max_threads()? as usize;
        let executors = ctx.try_get_cluster()?.get_nodes()?;

        // Get the source plan node by walk
        let mut source_plan = ReadDataSourcePlan::empty();
        {
            plan.walk_preorder(|plan| -> Result<bool> {
                match plan {
                    PlanNode::ReadSource(node) => {
                        source_plan = node.clone();
                        Ok(false)
                    }
                    _ => Ok(true),
                }
            })?;
        }

        // Local mode.
        {
            // Executor is empty
            if executors.is_empty() {
                return Ok(vec![]);
            }

            // Local table.
            let datasource = ctx.get_datasource();
            if datasource
                .get_table(source_plan.db.as_str(), source_plan.table.as_str())?
                .is_local()
            {
                return Ok(vec![]);
            }

            // Partition numbers <= current node cpus, in local mode
            if max_threads > source_plan.partitions.len() {
                return Ok(vec![]);
            }
        }

        // Remote mode.
        {
            let priority_sum = if executors.is_empty() {
                0
            } else {
                executors.iter().map(|n| n.priority as usize).sum()
            };

            let mut index = 0;
            let mut chunk_size;
            let mut num_chunks_so_far = 0;
            let total_chunks = source_plan.partitions.len();

            info!(
                "Schedule all [{:?}] partitions to [{:?}] nodes, all priority: [{:?}]",
                total_chunks,
                executors.len(),
                priority_sum
            );

            let all_parts = source_plan.partitions.clone();
            while num_chunks_so_far < total_chunks {
                let executor = &executors[index];
                let mut new_source_plan = source_plan.clone();
                // We have at lease one node
                if priority_sum > 0 {
                    let p_usize = executor.priority as usize;
                    let remainder = (p_usize * total_chunks) % priority_sum;
                    let left = total_chunks - num_chunks_so_far;
                    chunk_size = min(
                        (p_usize * total_chunks - remainder) / priority_sum + 1,
                        left,
                    );

                    info!(
                        "Executor[addr: {:?}, priority [{:?}] assigned [{:?}] partitions",
                        executor.address, executor.priority, chunk_size
                    );
                    index += 1;
                } else {
                    chunk_size = total_chunks;
                }
                new_source_plan.partitions = vec![];
                new_source_plan.partitions.extend_from_slice(
                    &all_parts[num_chunks_so_far..num_chunks_so_far + chunk_size],
                );
                num_chunks_so_far += chunk_size;

                // Walk and rewrite the plan from the source.
                let rewriter = ReadSourceRewriter { new_source_plan };
                let rewritten_node = rewriter.rewrite(plan)?;
                results.push((executor.address.clone(), rewritten_node));
            }
        }
        info!("Schedule plans to [{:?}] executors", results.len());

        Ok(results)
    }
}
