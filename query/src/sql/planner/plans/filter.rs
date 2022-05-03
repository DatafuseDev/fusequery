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

use std::any::Any;

use crate::sql::optimizer::PhysicalProperty;
use crate::sql::optimizer::RelationalProperty;
use crate::sql::optimizer::SExpr;
use crate::sql::planner::binder::ScalarExprRef;
use crate::sql::plans::BasePlan;
use crate::sql::plans::LogicalPlan;
use crate::sql::plans::PhysicalPlan;
use crate::sql::plans::PlanType;

#[derive(Clone)]
pub struct FilterPlan {
    // TODO: split predicate into conjunctions
    pub predicate: ScalarExprRef,
    // True if the plan represents having, else the plan represents where
    pub is_having: bool,
}

impl BasePlan for FilterPlan {
    fn plan_type(&self) -> PlanType {
        PlanType::Filter
    }

    fn is_physical(&self) -> bool {
        true
    }

    fn is_logical(&self) -> bool {
        true
    }

    fn as_physical(&self) -> Option<&dyn PhysicalPlan> {
        todo!()
    }

    fn as_logical(&self) -> Option<&dyn LogicalPlan> {
        todo!()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl PhysicalPlan for FilterPlan {
    fn compute_physical_prop(&self, _expression: &SExpr) -> PhysicalProperty {
        todo!()
    }
}

impl LogicalPlan for FilterPlan {
    fn compute_relational_prop(&self, _expression: &SExpr) -> RelationalProperty {
        todo!()
    }
}
