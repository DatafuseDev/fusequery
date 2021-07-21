// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_planners::PlanNode;
use common_tracing::tracing;

use crate::optimizers::optimizer_scatters::ScattersOptimizer;
use crate::optimizers::ConstantFoldingOptimizer;
use crate::optimizers::ProjectionPushDownOptimizer;
use crate::optimizers::StatisticsExactOptimizer;
use crate::sessions::FuseQueryContextRef;

pub trait Optimizer {
    fn name(&self) -> &str;
    fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode>;
}

pub struct Optimizers {
    inner: Vec<Box<dyn Optimizer>>,
}

impl Optimizers {
    pub fn create(ctx: FuseQueryContextRef) -> Self {
        let mut optimizers = Self::without_scatters(ctx.clone());
        optimizers
            .inner
            .push(Box::new(ScattersOptimizer::create(ctx)));
        optimizers
    }

    pub fn without_scatters(ctx: FuseQueryContextRef) -> Self {
        Optimizers {
            inner: vec![
                Box::new(ConstantFoldingOptimizer::create(ctx.clone())),
                Box::new(ProjectionPushDownOptimizer::create(ctx.clone())),
                Box::new(StatisticsExactOptimizer::create(ctx)),
            ],
        }
    }

    pub fn optimize(&mut self, plan: &PlanNode) -> Result<PlanNode> {
        let mut plan = plan.clone();
        for optimizer in self.inner.iter_mut() {
            tracing::debug!("Before {} \n{:?}", optimizer.name(), plan);
            plan = optimizer.optimize(&plan)?;
            tracing::debug!("After {} \n{:?}", optimizer.name(), plan);
        }
        Ok(plan)
    }
}
