// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_streams::SendableDataBlockStream;

#[async_trait::async_trait]
pub trait Interpreter: Sync + Send {
    fn name(&self) -> &str;
    async fn execute(&self) -> Result<SendableDataBlockStream>;

    fn schema(&self) -> DataSchemaRef {
        DataSchemaRefExt::create(vec![])
    }
}

pub type InterpreterPtr = std::sync::Arc<dyn Interpreter>;
