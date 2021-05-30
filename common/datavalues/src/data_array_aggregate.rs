// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::ErrorCodes;
use common_exception::Result;

use crate::DataArrayRef;
use crate::DataType;
use crate::DataValue;
use crate::DataValueAggregateOperator;
use crate::Float32Array;
use crate::Float64Array;
use crate::Int16Array;
use crate::Int32Array;
use crate::Int64Array;
use crate::Int8Array;
use crate::StringArray;
use crate::UInt16Array;
use crate::UInt32Array;
use crate::UInt64Array;
use crate::UInt8Array;

pub struct DataArrayAggregate;

impl DataArrayAggregate {
    #[inline]
    pub fn data_array_aggregate_op(
        op: DataValueAggregateOperator,
        value: DataArrayRef
    ) -> Result<DataValue> {
        match value.data_type() {
            DataType::Int8 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, Int8Array, Int8, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, Int8Array, Int8, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, Int8Array, Int8)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }
                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int8Array,
                        Int8,
                        i8,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int8Array,
                        Int8,
                        i8,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::Int16 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, Int16Array, Int16, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, Int16Array, Int16, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, Int16Array, Int16)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }
                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int16Array,
                        Int16,
                        i16,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int16Array,
                        Int16,
                        i16,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::Int32 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, Int32Array, Int32, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, Int32Array, Int32, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, Int32Array, Int32)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }

                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int32Array,
                        Int32,
                        i32,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int32Array,
                        Int32,
                        i32,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::Int64 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, Int64Array, Int64, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, Int64Array, Int64, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, Int64Array, Int64)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }

                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int64Array,
                        Int64,
                        i64,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Int64Array,
                        Int64,
                        i64,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::UInt8 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, UInt8Array, UInt8, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, UInt8Array, UInt8, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, UInt8Array, UInt8)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }

                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt8Array,
                        UInt8,
                        u8,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt8Array,
                        UInt8,
                        u8,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::UInt16 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, UInt16Array, UInt16, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, UInt16Array, UInt16, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, UInt16Array, UInt16)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }

                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt16Array,
                        UInt16,
                        u16,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt16Array,
                        UInt16,
                        u16,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::UInt32 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, UInt32Array, UInt32, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, UInt32Array, UInt32, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, UInt32Array, UInt32)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }

                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt32Array,
                        UInt32,
                        u32,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt32Array,
                        UInt32,
                        u32,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::UInt64 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, UInt64Array, UInt64, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, UInt64Array, UInt64, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, UInt64Array, UInt64)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }
                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt64Array,
                        UInt64,
                        u64,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        UInt64Array,
                        UInt64,
                        u64,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::Float32 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, Float32Array, Float32, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, Float32Array, Float32, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, Float32Array, Float32)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }
                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Float32Array,
                        Float32,
                        f32,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Float32Array,
                        Float32,
                        f32,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::Float64 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_to_data_value!(value, Float64Array, Float64, min)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_to_data_value!(value, Float64Array, Float64, max)
                }
                DataValueAggregateOperator::Sum => {
                    typed_array_sum_to_data_value!(value, Float64Array, Float64)
                }
                DataValueAggregateOperator::Count => {
                    Result::Ok(DataValue::UInt64(Some(value.len() as u64)))
                }
                DataValueAggregateOperator::Avg => {
                    Result::Err(ErrorCodes::BadDataValueType(format!(
                        "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                        op,
                        value.data_type()
                    )))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Float64Array,
                        Float64,
                        f64,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_to_data_value!(
                        value,
                        Float64Array,
                        Float64,
                        f64,
                        DataValueAggregateOperator::ArgMin
                    )
                }
            },
            DataType::Utf8 => match op {
                DataValueAggregateOperator::Min => {
                    typed_array_min_max_string_to_data_value!(value, StringArray, Utf8, min_string)
                }
                DataValueAggregateOperator::Max => {
                    typed_array_min_max_string_to_data_value!(value, StringArray, Utf8, max_string)
                }
                DataValueAggregateOperator::Count => {
                    Ok(DataValue::UInt64(Some(value.len() as u64)))
                }
                DataValueAggregateOperator::ArgMax => {
                    typed_array_values_min_max_string_to_data_value!(
                        value,
                        StringArray,
                        Utf8,
                        DataValueAggregateOperator::ArgMax
                    )
                }
                DataValueAggregateOperator::ArgMin => {
                    typed_array_values_min_max_string_to_data_value!(
                        value,
                        StringArray,
                        Utf8,
                        DataValueAggregateOperator::ArgMin
                    )
                }
                _ => Result::Err(ErrorCodes::BadDataValueType(format!(
                    "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                    op,
                    value.data_type()
                )))
            },
            _not_support_data_type => Result::Err(ErrorCodes::BadDataValueType(format!(
                "DataValue Error: Unsupported data_array_{} for data type: {:?}",
                op,
                value.data_type()
            )))
        }
    }
}
