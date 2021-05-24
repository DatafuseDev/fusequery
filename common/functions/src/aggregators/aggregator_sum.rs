// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;

use common_datablocks::DataBlock;
use common_datavalues::DataArrayAggregate;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::DataValueAggregateOperator;
use common_datavalues::DataValueArithmetic;
use common_datavalues::DataValueArithmeticOperator;
use common_exception::ErrorCodes;
use common_exception::Result;

use crate::IFunction;

#[derive(Clone)]
pub struct AggregatorSumFunction {
    display_name: String,
    depth: usize,
    arg: Box<dyn IFunction>,
    state: DataValue,
}

impl AggregatorSumFunction {
    pub fn try_create(
        display_name: &str,
        args: &[Box<dyn IFunction>],
    ) -> Result<Box<dyn IFunction>> {
        match args.len() {
            1 => Ok(Box::new(AggregatorSumFunction {
                display_name: display_name.to_string(),
                depth: 0,
                arg: args[0].clone(),
                state: DataValue::Null,
            })),
            _ => Result::Err(ErrorCodes::BadArguments(format!(
                "Function Error: Aggregator function {} args require single argument",
                display_name
            ))),
        }
    }
}

impl IFunction for AggregatorSumFunction {
    fn name(&self) -> &str {
        "AggregatorSumFunction"
    }

    fn return_type(&self, input_schema: &DataSchema) -> Result<DataType> {
        self.arg.return_type(input_schema)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn set_depth(&mut self, depth: usize) {
        self.depth = depth;
    }

    fn accumulate(&mut self, block: &DataBlock) -> Result<()> {
        let rows = block.num_rows();
        let val = self.arg.eval(&block)?;

        self.state = DataValueArithmetic::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            DataArrayAggregate::data_array_aggregate_op(
                DataValueAggregateOperator::Sum,
                val.to_array(rows)?,
            )?,
        )?;

        Ok(())
    }

    fn accumulate_result(&self) -> Result<Vec<DataValue>> {
        Ok(vec![self.state.clone()])
    }

    fn merge(&mut self, states: &[DataValue]) -> Result<()> {
        let val = states[self.depth].clone();
        self.state = DataValueArithmetic::data_value_arithmetic_op(
            DataValueArithmeticOperator::Plus,
            self.state.clone(),
            val,
        )?;
        Ok(())
    }

    fn merge_result(&self) -> Result<DataValue> {
        Ok(self.state.clone())
    }

    fn is_aggregator(&self) -> bool {
        true
    }
}

impl fmt::Display for AggregatorSumFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}({})", self.display_name, self.arg)
    }
}
