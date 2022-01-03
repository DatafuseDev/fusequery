// Copyright 2021 Datafuse Labs.
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

use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;

use super::arithmetic::ArithmeticTrait;
use super::arithmetic_mul::arithmetic_mul_div_monotonicity;
use crate::binary_arithmetic;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::scalars::Monotonicity;
use crate::with_match_primitive_type;

#[derive(Clone)]
pub struct ArithmeticDiv<T, D> {
    t: PhantomData<T>,
    d: PhantomData<D>,
}

impl<T, D> ArithmeticTrait for ArithmeticDiv<T, D>
where
    T: DFPrimitiveType + AsPrimitive<f64>,
    D: DFPrimitiveType + AsPrimitive<f64>,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        binary_arithmetic!(
            columns[0].column(),
            columns[1].column(),
            f64,
            |l: f64, r: f64| l / r
        )
    }
}

pub struct ArithmeticDivFunction;

impl ArithmeticDivFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[DataTypeAndNullable],
    ) -> Result<Box<dyn Function>> {
        let left_type = &args[0].data_type();
        let right_type = &args[1].data_type();
        let op = DataValueBinaryOperator::Div;
        let error_fn = || -> Result<Box<dyn Function>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
                left_type, op, right_type
            )))
        };

        // error on any non-numeric type
        if !left_type.is_numeric() || !right_type.is_numeric() {
            return error_fn();
        };

        let nullable = left_type.is_nullable() || right_type.is_nullable();

        with_match_primitive_type!(left_type, |$T| {
            with_match_primitive_type!(right_type, |$D| {
                BinaryArithmeticFunction::<ArithmeticDiv::<$T,$D>>::try_create_func(
                    op,
                    DataType::Float64(nullable),
                )
            }, {
                error_fn()
            })
        }, {
            error_fn()
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func)).features(
            FunctionFeatures::default()
                .deterministic()
                .monotonicity()
                .num_arguments(2),
        )
    }

    pub fn get_monotonicity(args: &[Monotonicity]) -> Result<Monotonicity> {
        arithmetic_mul_div_monotonicity(args, DataValueBinaryOperator::Div)
    }
}
