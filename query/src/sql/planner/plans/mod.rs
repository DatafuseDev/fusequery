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

mod aggregate;
mod apply;
mod eval_scalar;
mod filter;
mod hash_join;
mod limit;
mod logical_get;
mod logical_join;
mod max_one_row;
mod operator;
mod pattern;
mod physical_scan;
mod project;
mod scalar;
mod sort;

pub use aggregate::AggregatePlan;
pub use apply::CrossApply;
use common_ast::ast::ExplainKind;
use common_planners::*;
pub use eval_scalar::EvalScalar;
pub use eval_scalar::ScalarItem;
pub use filter::Filter;
pub use hash_join::PhysicalHashJoin;
pub use limit::LimitPlan;
pub use logical_get::LogicalGet;
pub use logical_join::JoinType;
pub use logical_join::LogicalInnerJoin;
pub use max_one_row::Max1Row;
pub use operator::*;
pub use pattern::PatternPlan;
pub use physical_scan::PhysicalScan;
pub use project::Project;
pub use scalar::*;
pub use sort::SortItem;
pub use sort::SortPlan;

use super::BindContext;
use super::MetadataRef;
use crate::sql::optimizer::SExpr;

#[derive(Clone)]
pub enum Plan {
    // `SELECT` statement
    Query {
        s_expr: SExpr,
        metadata: MetadataRef,
        bind_context: Box<BindContext>,
    },

    Explain {
        kind: ExplainKind,
        plan: Box<Plan>,
    },

    // System
    ShowMetrics,
    ShowProcessList,
    ShowSettings,

    // Databases
    CreateDatabase(Box<CreateDatabasePlan>),
    DropDatabase(Box<DropDatabasePlan>),
    RenameDatabase(Box<RenameDatabasePlan>),

    // Tables
    CreateTable(Box<CreateTablePlan>),

    // Views
    CreateView(Box<CreateViewPlan>),
    AlterView(Box<AlterViewPlan>),

    // DCL
    AlterUser(Box<AlterUserPlan>),
    CreateUser(Box<CreateUserPlan>),
    DropUser(Box<DropUserPlan>),

    // Stages
    ShowStages,
    ListStage(Box<ListPlan>),
    DescribeStage(Box<DescribeUserStagePlan>),
    CreateStage(Box<CreateUserStagePlan>),
    DropStage(Box<DropUserStagePlan>),
    RemoveStage(Box<RemoveUserStagePlan>),
}
