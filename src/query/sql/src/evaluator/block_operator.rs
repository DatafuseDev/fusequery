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

use common_catalog::plan::AggIndexMeta;
use common_exception::Result;
use common_expression::build_range_selection;
use common_expression::build_select_expr;
use common_expression::filter::SelectStrategy;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::Transform;
use common_pipeline_transforms::processors::Transformer;

use crate::optimizer::ColumnSet;

/// `BlockOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone)]
pub enum BlockOperator {
    /// Batch mode of map which merges map operators into one.
    Map {
        exprs: Vec<Expr>,
        /// The index of the output columns, based on the exprs.
        projections: Option<ColumnSet>,
    },

    /// Filter the input [`DataBlock`] with the predicate `eval`.
    Filter { projections: ColumnSet, expr: Expr },

    /// Reorganize the input [`DataBlock`] with `projection`.
    Project { projection: Vec<FieldIndex> },
}

impl BlockOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: DataBlock) -> Result<DataBlock> {
        if input.is_empty() {
            return Ok(input);
        }
        match self {
            BlockOperator::Map { exprs, projections } => {
                let num_evals = input
                    .get_meta()
                    .and_then(AggIndexMeta::downcast_ref_from)
                    .map(|a| a.num_evals);

                if let Some(num_evals) = num_evals {
                    // It's from aggregating index.
                    match projections {
                        Some(projections) => {
                            Ok(input.project_with_agg_index(projections, num_evals))
                        }
                        None => Ok(input),
                    }
                } else {
                    for expr in exprs {
                        let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                        let result = evaluator.run(expr)?;
                        let col = BlockEntry::new(expr.data_type().clone(), result);
                        input.add_column(col);
                    }
                    match projections {
                        Some(projections) => Ok(input.project(projections)),
                        None => Ok(input),
                    }
                }
            }

            BlockOperator::Filter { projections, expr } => {
                assert_eq!(expr.data_type(), &DataType::Boolean);

                let num_evals = input
                    .get_meta()
                    .and_then(AggIndexMeta::downcast_ref_from)
                    .map(|a| a.num_evals);

                if let Some(num_evals) = num_evals {
                    // It's from aggregating index.
                    Ok(input.project_with_agg_index(projections, num_evals))
                } else {
                    let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                    let (select_expr, has_or) = build_select_expr(expr);
                    // TODO(Dousir9): reuse the selection buffer
                    let mut true_selection = vec![0; input.num_rows()];
                    let mut false_selection = if has_or {
                        vec![0; input.num_rows()]
                    } else {
                        vec![]
                    };
                    let mut true_idx = 0;
                    let mut false_idx = 0;
                    let count = evaluator.process_selection(
                        &select_expr,
                        None,
                        &mut true_selection,
                        (&mut false_selection, false),
                        &mut true_idx,
                        &mut false_idx,
                        SelectStrategy::ALL,
                        input.num_rows(),
                    )?;

                    let data_block = input.project(projections);
                    if count == data_block.num_rows() {
                        Ok(data_block)
                    } else if count as f64 > data_block.num_rows() as f64 * 0.8
                        && data_block.num_columns() > 1
                    {
                        let selection_ranges = build_range_selection(&true_selection, count);
                        data_block.take_ranges(&selection_ranges, count)
                    } else {
                        data_block.take(&true_selection[0..count], &mut None)
                    }
                }
            }

            BlockOperator::Project { projection } => {
                let mut result = DataBlock::new(vec![], input.num_rows());
                for index in projection {
                    result.add_column(input.get_by_offset(*index).clone());
                }
                Ok(result)
            }
        }
    }
}

/// `CompoundBlockOperator` is a pipeline of `BlockOperator`s
pub struct CompoundBlockOperator {
    pub operators: Vec<BlockOperator>,
    pub ctx: FunctionContext,
}

impl CompoundBlockOperator {
    pub fn new(
        operators: Vec<BlockOperator>,
        ctx: FunctionContext,
        input_num_columns: usize,
    ) -> Self {
        let operators = Self::compact_map(operators, input_num_columns);
        Self { operators, ctx }
    }

    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        input_num_columns: usize,
        ctx: FunctionContext,
        operators: Vec<BlockOperator>,
    ) -> Box<dyn Processor> {
        let operators = Self::compact_map(operators, input_num_columns);
        Transformer::<Self>::create(input_port, output_port, Self { operators, ctx })
    }

    pub fn compact_map(
        operators: Vec<BlockOperator>,
        input_num_columns: usize,
    ) -> Vec<BlockOperator> {
        let mut results = Vec::with_capacity(operators.len());

        for op in operators {
            match op {
                BlockOperator::Map { exprs, projections } => {
                    if let Some(BlockOperator::Map {
                        exprs: pre_exprs,
                        projections: pre_projections,
                    }) = results.last_mut()
                    {
                        if pre_projections.is_none() && projections.is_none() {
                            pre_exprs.extend(exprs);
                        } else {
                            results.push(BlockOperator::Map { exprs, projections });
                        }
                    } else {
                        results.push(BlockOperator::Map { exprs, projections });
                    }
                }
                _ => results.push(op),
            }
        }

        crate::evaluator::cse::apply_cse(results, input_num_columns)
    }
}

impl Transform for CompoundBlockOperator {
    const NAME: &'static str = "CompoundBlockOperator";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        self.operators
            .iter()
            .try_fold(data_block, |input, op| op.execute(&self.ctx, input))
    }

    fn name(&self) -> String {
        format!(
            "{}({})",
            Self::NAME,
            self.operators
                .iter()
                .map(|op| {
                    match op {
                        BlockOperator::Map { .. } => "Map",
                        BlockOperator::Filter { .. } => "Filter",
                        BlockOperator::Project { .. } => "Project",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
