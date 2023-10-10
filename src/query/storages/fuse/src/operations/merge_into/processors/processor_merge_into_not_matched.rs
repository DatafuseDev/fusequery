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
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchemaRef;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::pipe::PipeItem;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_sql::evaluator::BlockOperator;
use common_storage::metrics::merge_into::merge_into_not_matched_operation_milliseconds;
use common_storage::metrics::merge_into::metrics_inc_merge_into_append_blocks_counter;
use common_storage::metrics::merge_into::metrics_inc_merge_into_append_blocks_rows_counter;
use itertools::Itertools;

use crate::operations::merge_into::mutator::SplitByExprMutator;
// (source_schema,condition,values_exprs)
type UnMatchedExprs = Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>;

struct InsertDataBlockMutation {
    op: BlockOperator,
    split_mutator: SplitByExprMutator,
}

// need to evaluate expression and

pub struct MergeIntoNotMatchedProcessor {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    ops: Vec<InsertDataBlockMutation>,
    input_data: Option<DataBlock>,
    output_data: Vec<DataBlock>,
    func_ctx: FunctionContext,
    // data_schemas[i] means the i-th op's result block's schema.
    data_schemas: HashMap<usize, DataSchemaRef>,
}

impl MergeIntoNotMatchedProcessor {
    pub fn create(
        unmatched: UnMatchedExprs,
        input_schema: DataSchemaRef,
        func_ctx: FunctionContext,
    ) -> Result<Self> {
        let mut ops = Vec::<InsertDataBlockMutation>::with_capacity(unmatched.len());
        let mut data_schemas = HashMap::with_capacity(unmatched.len());
        for (idx, item) in unmatched.iter().enumerate() {
            let eval_projections: HashSet<usize> =
                (input_schema.num_fields()..input_schema.num_fields() + item.2.len()).collect();

            data_schemas.insert(idx, item.0.clone());
            ops.push(InsertDataBlockMutation {
                op: BlockOperator::Map {
                    exprs: item
                        .2
                        .iter()
                        .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
                        .collect_vec(),
                    projections: Some(eval_projections),
                },
                split_mutator: {
                    let filter = item.1.as_ref().map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS));
                    SplitByExprMutator::create(filter, func_ctx.clone())
                },
            });
        }

        Ok(Self {
            input_port: InputPort::create(),
            output_port: OutputPort::create(),
            ops,
            input_data: None,
            output_data: Vec::new(),
            func_ctx,
            data_schemas,
        })
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input_port.clone();
        let output_port = self.output_port.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output_port])
    }
}

impl Processor for MergeIntoNotMatchedProcessor {
    fn name(&self) -> String {
        "MergeIntoNotMatched".to_owned()
    }

    #[doc = " Reference used for downcast."]
    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        let finished = self.input_port.is_finished() && self.output_data.is_empty();
        if finished {
            self.output_port.finish();
            return Ok(Event::Finished);
        }

        let mut pushed_something = false;

        if self.output_port.can_push() && !self.output_data.is_empty() {
            self.output_port
                .push_data(Ok(self.output_data.pop().unwrap()));
            pushed_something = true
        }

        if pushed_something {
            return Ok(Event::NeedConsume);
        }

        if self.input_port.has_data() {
            if self.output_data.is_empty() {
                self.input_data = Some(self.input_port.pull_data().unwrap()?);
                Ok(Event::Sync)
            } else {
                Ok(Event::NeedConsume)
            }
        } else {
            self.input_port.set_need_data();
            Ok(Event::NeedData)
        }
    }

    fn process(&mut self) -> Result<()> {
        if let Some(data_block) = self.input_data.take() {
            if data_block.is_empty() {
                return Ok(());
            }
            let start = Instant::now();
            let mut current_block = data_block;
            for (idx, op) in self.ops.iter().enumerate() {
                let (mut satisfied_block, unsatisfied_block) =
                    op.split_mutator.split_by_expr(current_block)?;
                satisfied_block = satisfied_block
                    .add_meta(Some(Box::new(self.data_schemas.get(&idx).unwrap().clone())))?;
                if !satisfied_block.is_empty() {
                    metrics_inc_merge_into_append_blocks_counter(1);
                    metrics_inc_merge_into_append_blocks_rows_counter(
                        satisfied_block.num_rows() as u32
                    );
                    self.output_data
                        .push(op.op.execute(&self.func_ctx, satisfied_block)?)
                }

                if unsatisfied_block.is_empty() {
                    break;
                } else {
                    current_block = unsatisfied_block
                }
            }
            let elapsed_time = start.elapsed().as_millis() as u64;
            merge_into_not_matched_operation_milliseconds(elapsed_time);
        }

        Ok(())
    }
}
