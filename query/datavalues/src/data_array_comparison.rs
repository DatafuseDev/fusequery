// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use crate::error::DataValueResult;
use crate::{DataArrayRef, DataColumnarValue, DataType, DataValueComparisonOperator};
use crate::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};

pub fn data_array_comparison_op(
    op: DataValueComparisonOperator,
    left: &DataColumnarValue,
    right: &DataColumnarValue,
) -> DataValueResult<DataArrayRef> {
    match (left, right) {
        (DataColumnarValue::Array(left_array), DataColumnarValue::Array(right_array)) => {
            let coercion_type = super::data_type::equal_coercion(
                &left_array.data_type(),
                &right_array.data_type(),
            )?;
            let left_array = arrow::compute::cast(&left_array, &coercion_type)?;
            let right_array = arrow::compute::cast(&right_array, &coercion_type)?;

            match op {
                DataValueComparisonOperator::Eq => arrow_array_op!(&left_array, &right_array, eq),
                DataValueComparisonOperator::Lt => arrow_array_op!(&left_array, &right_array, lt),
                DataValueComparisonOperator::LtEq => {
                    arrow_array_op!(&left_array, &right_array, lt_eq)
                }
                DataValueComparisonOperator::Gt => arrow_array_op!(&left_array, &right_array, gt),
                DataValueComparisonOperator::GtEq => {
                    arrow_array_op!(&left_array, &right_array, gt_eq)
                }
                DataValueComparisonOperator::NotEq => {
                    arrow_array_op!(&left_array, &right_array, neq)
                }
            }
        }

        (DataColumnarValue::Array(array), DataColumnarValue::Scalar(scalar)) => {
            let coercion_type =
                super::data_type::equal_coercion(&array.data_type(), &scalar.data_type())?;
            let left_array = arrow::compute::cast(&array, &coercion_type)?;
            let right_array = arrow::compute::cast(&scalar.to_array(1)?, &coercion_type)?;
            let scalar = super::DataValue::try_from_array(&right_array, 0)?;

            match op {
                DataValueComparisonOperator::Eq => arrow_array_op_scalar!(left_array, scalar, eq),
                DataValueComparisonOperator::Lt => arrow_array_op_scalar!(left_array, scalar, lt),
                DataValueComparisonOperator::LtEq => {
                    arrow_array_op_scalar!(left_array, scalar, lt_eq)
                }
                DataValueComparisonOperator::Gt => arrow_array_op_scalar!(left_array, scalar, gt),
                DataValueComparisonOperator::GtEq => {
                    arrow_array_op_scalar!(left_array, scalar, gt_eq)
                }
                DataValueComparisonOperator::NotEq => {
                    arrow_array_op_scalar!(left_array, scalar, neq)
                }
            }
        }

        (DataColumnarValue::Scalar(scalar), DataColumnarValue::Array(array)) => {
            let coercion_type =
                super::data_type::equal_coercion(&array.data_type(), &scalar.data_type())?;
            let left_array = arrow::compute::cast(&scalar.to_array(1)?, &coercion_type)?;
            let right_array = arrow::compute::cast(&array, &coercion_type)?;
            let scalar = super::DataValue::try_from_array(&left_array, 0)?;

            match op {
                DataValueComparisonOperator::Eq => arrow_array_op_scalar!(right_array, scalar, eq),
                DataValueComparisonOperator::Lt => arrow_array_op_scalar!(right_array, scalar, gt),
                DataValueComparisonOperator::LtEq => {
                    arrow_array_op_scalar!(right_array, scalar, gt_eq)
                }
                DataValueComparisonOperator::Gt => arrow_array_op_scalar!(right_array, scalar, lt),
                DataValueComparisonOperator::GtEq => {
                    arrow_array_op_scalar!(right_array, scalar, lt_eq)
                }
                DataValueComparisonOperator::NotEq => {
                    arrow_array_op_scalar!(right_array, scalar, neq)
                }
            }
        }
        (DataColumnarValue::Scalar(left_scala), DataColumnarValue::Scalar(right_scalar)) => {
            let coercion_type = super::data_type::equal_coercion(
                &left_scala.data_type(),
                &right_scalar.data_type(),
            )?;
            let left_array = arrow::compute::cast(&left_scala.to_array(1)?, &coercion_type)?;
            let right_array = arrow::compute::cast(&right_scalar.to_array(1)?, &coercion_type)?;

            match op {
                DataValueComparisonOperator::Eq => arrow_array_op!(&left_array, &right_array, eq),
                DataValueComparisonOperator::Lt => arrow_array_op!(&left_array, &right_array, lt),
                DataValueComparisonOperator::LtEq => {
                    arrow_array_op!(&left_array, &right_array, lt_eq)
                }
                DataValueComparisonOperator::Gt => arrow_array_op!(&left_array, &right_array, gt),
                DataValueComparisonOperator::GtEq => {
                    arrow_array_op!(&left_array, &right_array, gt_eq)
                }
                DataValueComparisonOperator::NotEq => {
                    arrow_array_op!(&left_array, &right_array, neq)
                }
            }
        }
    }
}
