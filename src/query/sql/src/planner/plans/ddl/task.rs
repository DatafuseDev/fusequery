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

use common_ast::ast::AlterTaskOptions;
use common_ast::ast::ScheduleOptions;
use common_ast::ast::ShowLimit;
use common_ast::ast::WarehouseOptions;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateTaskPlan {
    pub if_not_exists: bool,
    pub tenant: String,
    pub task_name: String,
    pub warehouse_opts: WarehouseOptions,
    pub schedule_opts: ScheduleOptions,
    pub suspend_task_after_num_failures: Option<u64>,
    pub sql: String,
    pub comment: String,
}

impl CreateTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterTaskPlan {
    pub if_exists: bool,
    pub tenant: String,
    pub task_name: String,
    pub alter_options: AlterTaskOptions,
}

impl AlterTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropTaskPlan {
    pub if_exists: bool,
    pub tenant: String,
    pub task_name: String,
}

impl DropTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DescribeTaskPlan {
    pub tenant: String,
    pub task_name: String,
}

impl DescribeTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecuteTaskPlan {
    pub tenant: String,
    pub task_name: String,
}

impl ExecuteTaskPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShowTasksPlan {
    pub tenant: String,
    pub limit: Option<ShowLimit>,
}

impl ShowTasksPlan {
    pub fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}
