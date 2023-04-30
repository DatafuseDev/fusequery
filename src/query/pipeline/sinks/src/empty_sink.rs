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

use std::sync::Arc;

use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::Processor;

use super::Sink;
use super::Sinker;

pub struct EmptySink;

impl EmptySink {
    pub fn create(input: Arc<InputPort>) -> Box<dyn Processor> {
        Sinker::create(input, EmptySink {})
    }
}

impl Sink for EmptySink {
    const NAME: &'static str = "EmptySink";

    fn consume(&mut self, _: DataBlock) -> Result<()> {
        Ok(())
    }
}
