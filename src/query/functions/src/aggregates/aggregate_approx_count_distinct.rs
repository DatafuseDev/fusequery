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

use std::hash::Hash;
use std::sync::Arc;

use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::UInt64Type;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::Scalar;
use streaming_algorithms::HyperLogLog;

use super::aggregate_function::AggregateFunction;
use super::aggregate_function_factory::AggregateFunctionDescription;
use super::deserialize_state;
use super::serialize_state;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::aggregator_common::assert_unary_arguments;

/// Use Hyperloglog to estimate distinct of values
struct AggregateApproxCountDistinctState<T>
where T: ValueType
{
    hll: HyperLogLog<T::Scalar>,
}

impl<T> Default for AggregateApproxCountDistinctState<T>
where
    T: ValueType + Send + Sync,
    T::Scalar: Hash,
{
    fn default() -> Self {
        Self {
            hll: HyperLogLog::<T::Scalar>::new(0.04),
        }
    }
}

impl<T> UnaryState<T, UInt64Type> for AggregateApproxCountDistinctState<T>
where
    T: ValueType + Send + Sync,
    T::Scalar: Hash,
{
    fn add(&mut self, other: T::ScalarRef<'_>) -> Result<()> {
        self.hll.push(&T::to_owned_scalar(other));
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.hll.union(&rhs.hll);
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut Vec<u64>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        builder.push(self.hll.len() as u64);
        Ok(())
    }

    fn serialize(&self, writer: &mut Vec<u8>) -> Result<()> {
        serialize_state(writer, &self.hll)
    }

    fn deserialize(reader: &mut &[u8]) -> Result<Self>
    where Self: Sized {
        let hll = deserialize_state(reader)?;
        Ok(Self { hll })
    }
}

pub fn try_create_aggregate_approx_count_distinct_function(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let return_type = DataType::Number(NumberDataType::UInt64);

    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState<NumberType<NUM_TYPE>>,
                NumberType<NUM_TYPE>,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        DataType::String => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState<StringType>,
                StringType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        DataType::Date => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState<DateType>,
                DateType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        DataType::Timestamp => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState<TimestampType>,
                TimestampType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
        _ => {
            let func = AggregateUnaryFunction::<
                AggregateApproxCountDistinctState<AnyType>,
                AnyType,
                UInt64Type,
            >::try_create(
                display_name, return_type, params, arguments[0].clone()
            )
            .with_need_drop(true);

            Ok(Arc::new(func))
        }
    })
}

pub fn aggregate_approx_count_distinct_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        returns_default_when_only_null: true,
        ..Default::default()
    };

    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_approx_count_distinct_function),
        features,
    )
}
