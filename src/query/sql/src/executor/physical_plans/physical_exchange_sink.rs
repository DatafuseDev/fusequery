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

use common_exception::Result;
use common_expression::DataSchemaRef;
use common_expression::RemoteExpr;

use crate::executor::physical_plans::common::FragmentKind;
use crate::executor::PhysicalPlan;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ExchangeSink {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    // Input schema of exchanged data
    pub schema: DataSchemaRef,
    pub kind: FragmentKind,
    pub keys: Vec<RemoteExpr>,

    // Fragment ID of sink fragment
    pub destination_fragment_id: usize,

    // Addresses of destination nodes
    pub query_id: String,
    pub ignore_exchange: bool,
    pub allow_adjust_parallelism: bool,
}

impl ExchangeSink {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}
