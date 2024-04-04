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

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::DataBlock;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::evaluator::BlockOperator;

use crate::operations::get_not;

#[derive(Clone)]
pub enum SubqueryMutation {
    Delete,
    Update(Vec<BlockOperator>),
}

pub struct TransformMutationSubquery {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    mutation: SubqueryMutation,
    filter_executor: FilterExecutor,
    func_ctx: FunctionContext,
}

impl TransformMutationSubquery {
    pub fn try_create(
        func_ctx: FunctionContext,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        mutation: SubqueryMutation,
        filter_executor: FilterExecutor,
    ) -> Result<Self> {
        Ok(TransformMutationSubquery {
            input,
            output,
            input_data: None,
            output_data: None,
            mutation,
            filter_executor,
            func_ctx,
        })
    }

    pub fn into_processor(self) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(self)))
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input.clone();
        let output = self.output.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output])
    }

    fn get_filter(
        &self,
        predicate: Value<BooleanType>,
        data_block: &DataBlock,
    ) -> Result<(Value<BooleanType>, Value<BooleanType>)> {
        let (predicate_not, _) = get_not(predicate.clone(), &self.func_ctx, data_block.num_rows())?;
        let predicate_not = predicate_not.try_downcast().unwrap();
        Ok((predicate, predicate_not))
    }

    pub fn split_not_matched_block(&mut self, block: DataBlock) -> Result<DataBlock> {
        let data_block = self.filter_executor.mark(block.clone())?;

        let num_columns = data_block.num_columns();
        let filter_entry = data_block.get_by_offset(num_columns - 1);
        let value = filter_entry.value.clone();
        let predicate: Option<Value<BooleanType>> = value.try_downcast();
        if let Some(predicate) = predicate {
            let (_predicate, predicate_not) = self.get_filter(predicate, &data_block)?;

            let not_matched_block = block.filter_boolean_value(&predicate_not)?;

            let not_matched_block = DataBlock::new(
                not_matched_block.columns()[..num_columns - 1].to_vec(),
                not_matched_block.num_rows(),
            );
            Ok(not_matched_block)
        } else {
            Err(ErrorCode::from_string(
                "subquery filter type MUST be Column::Nullable(Boolean)".to_string(),
            ))
        }
    }
}

#[async_trait::async_trait]
impl Processor for TransformMutationSubquery {
    fn name(&self) -> String {
        "TransformMutationSubquery".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        // 1. if there is no data and input_port is finished, this processor has finished
        // it's work
        if self.input.is_finished() && self.output_data.is_none() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        // 2. process data stage here
        if self.output.can_push() {
            if let Some(data) = self.output_data.take() {
                self.output.push_data(Ok(data));
                return Ok(Event::NeedConsume);
            }
        }

        // 3. trigger down stream pipeItem to consume if we pushed data
        if self.input.has_data() {
            if self.output_data.is_none() {
                // no pending data (being sent to down streams)
                self.input_data = Some(self.input.pull_data().unwrap()?);
                return Ok(Event::Sync);
            } else {
                // data pending
                return Ok(Event::NeedConsume);
            }
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            if data_block.is_empty() {
                return Ok(());
            }

            let output_data = match &self.mutation {
                SubqueryMutation::Delete => self.split_not_matched_block(data_block)?,
                SubqueryMutation::Update(operators) => {
                    let data_block = self.filter_executor.mark(data_block)?;

                    operators
                        .iter()
                        .try_fold(data_block, |input, op| op.execute(&self.func_ctx, input))?
                }
            };

            self.output_data = Some(output_data);
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        Ok(())
    }
}
