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
use std::ops::Rem;

use common_datavalues::prelude::*;
use common_datavalues::DataTypeAndNullable;
use common_exception::ErrorCode;
use common_exception::Result;
use num::cast::AsPrimitive;

use super::arithmetic::ArithmeticTrait;
use super::utils::rem_scalar;
use crate::scalars::function_factory::ArithmeticDescription;
use crate::scalars::function_factory::FunctionFeatures;
use crate::scalars::BinaryArithmeticFunction;
use crate::scalars::Function;
use crate::try_binary_arithmetic;
use crate::with_match_primitive_type;

#[derive(Clone)]
pub struct ArithmeticModule<T, D, M, R> {
    t: PhantomData<T>,
    d: PhantomData<D>,
    m: PhantomData<M>,
    r: PhantomData<R>,
}

impl<T, D, M, R> ArithmeticTrait for ArithmeticModule<T, D, M, R>
where
    T: DFPrimitiveType + AsPrimitive<M>,
    D: DFPrimitiveType + AsPrimitive<M> + num::One,
    R: DFPrimitiveType,
    M: DFPrimitiveType + num::Zero + AsPrimitive<R> + Rem<Output = M>,
    u8: AsPrimitive<R>,
    u16: AsPrimitive<R>,
    u32: AsPrimitive<R>,
    u64: AsPrimitive<R>,
    DFPrimitiveArray<R>: IntoSeries,
    R: Into<DataValue>,
{
    fn arithmetic(columns: &DataColumnsWithField) -> Result<DataColumn> {
        let need_check = R::data_type().is_integer();
        try_binary_arithmetic! {
            columns[0].column(),
            columns[1].column(),
            M,
            |l: M, r:M| {
                if std::intrinsics::unlikely(need_check && r == M::zero()) {
                    return Err(ErrorCode::BadArguments("Division by zero"));
                }
                Ok(AsPrimitive::<R>::as_(l % r))
            },
            |lhs: &DFPrimitiveArray<T>, r: M| {
                if need_check && r == M::zero() {
                    return Err(ErrorCode::BadArguments("Division by zero"));
                }
                Ok(rem_scalar(lhs, &r).into())
            }
        }
    }
}

pub struct ArithmeticModuloFunction;

impl ArithmeticModuloFunction {
    pub fn try_create_func(
        _display_name: &str,
        args: &[DataTypeAndNullable],
    ) -> Result<Box<dyn Function>> {
        let left_type = &args[0].data_type();
        let right_type = &args[1].data_type();
        let op = DataValueBinaryOperator::Modulo;

        let error_fn = || -> Result<Box<dyn Function>> {
            Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Unsupported arithmetic ({:?}) {} ({:?})",
                left_type, op, right_type
            )))
        };

        if !left_type.is_numeric() || !right_type.is_numeric() {
            return error_fn();
        };

        with_match_primitive_type!(left_type, |$T| {
            with_match_primitive_type!(right_type, |$D| {
                let result_type = <($T, $D) as ResultTypeOfBinary>::Modulo::data_type();
                BinaryArithmeticFunction::<ArithmeticModule<$T, $D, <($T, $D) as ResultTypeOfBinary>::LeastSuper, <($T, $D) as ResultTypeOfBinary>::Modulo>>::try_create_func(
                    op,
                    result_type,
                )
            }, {
                error_fn()
            })
        }, {
            error_fn()
        })
    }

    pub fn desc() -> ArithmeticDescription {
        ArithmeticDescription::creator(Box::new(Self::try_create_func))
            .features(FunctionFeatures::default().deterministic().num_arguments(2))
    }
}
