// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_arrow::arrow::datatypes as arrow_data_types;
use common_arrow::arrow::datatypes::ArrowNumericType;
use common_arrow::arrow::datatypes::ArrowPrimitiveType;
use common_arrow::arrow::datatypes::IntervalUnit;
use common_arrow::arrow::datatypes::TimeUnit;

use crate::arrays::DataArray;
use crate::DataField;
use crate::DataType;

pub trait DFDataType: Send + Sync {
    fn data_type() -> DataType;
}

macro_rules! impl_df_datatype {
    ($ca:ident, $variant:ident) => {
        impl DFDataType for $ca {
            fn data_type() -> DataType {
                DataType::$variant
            }
        }
    };
}

pub type UInt8Type = arrow_data_types::UInt8Type;
pub type UInt16Type = arrow_data_types::UInt16Type;
pub type UInt32Type = arrow_data_types::UInt32Type;
pub type UInt64Type = arrow_data_types::UInt64Type;
pub type Int8Type = arrow_data_types::Int8Type;
pub type Int16Type = arrow_data_types::Int16Type;
pub type Int32Type = arrow_data_types::Int32Type;
pub type Int64Type = arrow_data_types::Int64Type;
pub type Float32Type = arrow_data_types::Float32Type;
pub type Float64Type = arrow_data_types::Float64Type;

pub type BooleanType = arrow_data_types::BooleanType;

pub type Date32Type = arrow_data_types::Date32Type;
pub type Date64Type = arrow_data_types::Date64Type;

pub type TimestampSecondType = arrow_data_types::TimestampSecondType;
pub type TimestampMillisecondType = arrow_data_types::TimestampMillisecondType;
pub type TimestampMicrosecondType = arrow_data_types::TimestampMicrosecondType;
pub type TimestampNanosecondType = arrow_data_types::TimestampNanosecondType;

pub type IntervalYearMonthType = arrow_data_types::IntervalYearMonthType;
pub type IntervalDayTimeType = arrow_data_types::IntervalDayTimeType;

pub struct NullType;
pub struct Utf8Type;
pub struct ListType;

pub struct StructType;
pub struct BinaryType;

pub type DFNullArray = DataArray<NullType>;
pub type DFInt8Array = DataArray<Int8Type>;
pub type DFUInt8Array = DataArray<UInt8Type>;
pub type DFInt16Array = DataArray<Int16Type>;
pub type DFUInt16Array = DataArray<UInt16Type>;
pub type DFInt32Array = DataArray<Int32Type>;
pub type DFUInt32Array = DataArray<UInt32Type>;
pub type DFInt64Array = DataArray<Int64Type>;
pub type DFUInt64Array = DataArray<UInt64Type>;

pub type DFBooleanArray = DataArray<BooleanType>;

pub type DFFloat32Array = DataArray<Float32Type>;
pub type DFFloat64Array = DataArray<Float64Type>;

pub type DFUtf8Array = DataArray<Utf8Type>;
pub type DFListArray = DataArray<ListType>;
pub type DFStructArray = DataArray<StructType>;
pub type DFBinaryArray = DataArray<BinaryType>;

pub type DFDate32Array = DataArray<Date32Type>;
pub type DFDate64Array = DataArray<Date64Type>;

pub type DFTimestampSecondArray = DataArray<TimestampSecondType>;
pub type DFTimestampMillisecondArray = DataArray<TimestampMillisecondType>;
pub type DFTimestampMicrosecondArray = DataArray<TimestampMicrosecondType>;
pub type DFTimestampNanosecondArray = DataArray<TimestampNanosecondType>;
pub type DFIntervalYearMonthArray = DataArray<IntervalYearMonthType>;
pub type DFIntervalDayTimeArray = DataArray<IntervalDayTimeType>;

impl_df_datatype!(UInt8Type, UInt8);
impl_df_datatype!(UInt16Type, UInt16);
impl_df_datatype!(UInt32Type, UInt32);
impl_df_datatype!(UInt64Type, UInt64);
impl_df_datatype!(Int8Type, Int8);
impl_df_datatype!(Int16Type, Int16);
impl_df_datatype!(Int32Type, Int32);
impl_df_datatype!(Int64Type, Int64);
impl_df_datatype!(Float32Type, Float32);
impl_df_datatype!(Float64Type, Float64);
impl_df_datatype!(BooleanType, Boolean);
impl_df_datatype!(Date32Type, Date32);
impl_df_datatype!(Date64Type, Date64);

impl DFDataType for TimestampSecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Second, None)
    }
}

impl DFDataType for TimestampMillisecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, None)
    }
}

impl DFDataType for TimestampMicrosecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    }
}

impl DFDataType for TimestampNanosecondType {
    fn data_type() -> DataType {
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    }
}

impl DFDataType for IntervalYearMonthType {
    fn data_type() -> DataType {
        DataType::Interval(IntervalUnit::YearMonth)
    }
}

impl DFDataType for IntervalDayTimeType {
    fn data_type() -> DataType {
        DataType::Interval(IntervalUnit::DayTime)
    }
}

impl DFDataType for Utf8Type {
    fn data_type() -> DataType {
        DataType::Utf8
    }
}

impl DFDataType for ListType {
    fn data_type() -> DataType {
        // null as we cannot no anything without self.
        DataType::List(Box::new(DataField::new("", DataType::Null, true)))
    }
}

impl DFDataType for NullType {
    fn data_type() -> DataType {
        DataType::Null
    }
}

impl DFDataType for BinaryType {
    fn data_type() -> DataType {
        // null as we cannot no anything without self.
        DataType::Binary
    }
}

impl DFDataType for StructType {
    fn data_type() -> DataType {
        // null as we cannot no anything without self.
        DataType::Struct(vec![DataField::new("", DataType::Null, true)])
    }
}

pub trait DFPrimitiveType: ArrowPrimitiveType + Send + Sync + DFDataType {}

impl DFPrimitiveType for UInt8Type {}
impl DFPrimitiveType for UInt16Type {}
impl DFPrimitiveType for UInt32Type {}
impl DFPrimitiveType for UInt64Type {}
impl DFPrimitiveType for Int8Type {}
impl DFPrimitiveType for Int16Type {}
impl DFPrimitiveType for Int32Type {}
impl DFPrimitiveType for Int64Type {}
impl DFPrimitiveType for Float32Type {}
impl DFPrimitiveType for Float64Type {}
impl DFPrimitiveType for Date32Type {}
impl DFPrimitiveType for Date64Type {}

impl DFPrimitiveType for TimestampSecondType {}
impl DFPrimitiveType for TimestampMillisecondType {}
impl DFPrimitiveType for TimestampMicrosecondType {}
impl DFPrimitiveType for TimestampNanosecondType {}

impl DFPrimitiveType for IntervalYearMonthType {}
impl DFPrimitiveType for IntervalDayTimeType {}

pub trait DFNumericType: DFPrimitiveType + ArrowNumericType {}

impl DFNumericType for UInt8Type {}
impl DFNumericType for UInt16Type {}
impl DFNumericType for UInt32Type {}
impl DFNumericType for UInt64Type {}
impl DFNumericType for Int8Type {}
impl DFNumericType for Int16Type {}
impl DFNumericType for Int32Type {}
impl DFNumericType for Int64Type {}
impl DFNumericType for Float32Type {}
impl DFNumericType for Float64Type {}
impl DFNumericType for Date32Type {}
impl DFNumericType for Date64Type {}

impl DFNumericType for TimestampSecondType {}
impl DFNumericType for TimestampMillisecondType {}
impl DFNumericType for TimestampMicrosecondType {}
impl DFNumericType for TimestampNanosecondType {}

impl DFNumericType for IntervalYearMonthType {}
impl DFNumericType for IntervalDayTimeType {}

pub trait DFIntegerType: DFNumericType {}
impl DFIntegerType for UInt8Type {}
impl DFIntegerType for UInt16Type {}
impl DFIntegerType for UInt32Type {}
impl DFIntegerType for UInt64Type {}
impl DFIntegerType for Int8Type {}
impl DFIntegerType for Int16Type {}
impl DFIntegerType for Int32Type {}
impl DFIntegerType for Int64Type {}
impl DFIntegerType for Date32Type {}
impl DFIntegerType for Date64Type {}

impl DFIntegerType for TimestampSecondType {}
impl DFIntegerType for TimestampMillisecondType {}
impl DFIntegerType for TimestampMicrosecondType {}
impl DFIntegerType for TimestampNanosecondType {}

impl DFIntegerType for IntervalYearMonthType {}
impl DFIntegerType for IntervalDayTimeType {}

pub trait DFFloatType: DFNumericType {}
impl DFFloatType for Float32Type {}
impl DFFloatType for Float64Type {}
