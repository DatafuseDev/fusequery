// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[macro_export]
macro_rules! dispatch_numeric_types {
    ($dispatch: ident, $data_type: expr, $($args:expr),*) => {
        $dispatch! { UInt8Type, $data_type, $($args),* }
        $dispatch! { UInt16Type, $data_type, $($args),* }
        $dispatch! { UInt32Type, $data_type, $($args),* }
        $dispatch! { UInt64Type, $data_type, $($args),* }
        $dispatch! { Int8Type, $data_type, $($args),* }
        $dispatch! { Int16Type, $data_type, $($args),* }
        $dispatch! { Int32Type, $data_type, $($args),* }
        $dispatch! { Int64Type, $data_type, $($args),* }
        $dispatch! { Float32Type, $data_type, $($args),* }
        $dispatch! { Float64Type, $data_type, $($args),* }
    };
}

#[macro_export]
macro_rules! match_data_type_apply_macro_ca {
    ($self:expr, $macro:ident, $macro_utf8:ident, $macro_bool:ident $(, $opt_args:expr)*) => {{
        use crate::DataType;
        match $self.data_type() {
            DataType::Utf8 => $macro_utf8!($self.utf8().unwrap() $(, $opt_args)*),
            DataType::Boolean => $macro_bool!($self.bool().unwrap() $(, $opt_args)*),
            DataType::UInt8 => $macro!($self.u8().unwrap() $(, $opt_args)*),
            DataType::UInt16 => $macro!($self.u16().unwrap() $(, $opt_args)*),
            DataType::UInt32 => $macro!($self.u32().unwrap() $(, $opt_args)*),
            DataType::UInt64 => $macro!($self.u64().unwrap() $(, $opt_args)*),
            DataType::Int8 => $macro!($self.i8().unwrap() $(, $opt_args)*),
            DataType::Int16 => $macro!($self.i16().unwrap() $(, $opt_args)*),
            DataType::Int32 => $macro!($self.i32().unwrap() $(, $opt_args)*),
            DataType::Int64 => $macro!($self.i64().unwrap() $(, $opt_args)*),
            DataType::Float32 => $macro!($self.f32().unwrap() $(, $opt_args)*),
            DataType::Float64 => $macro!($self.f64().unwrap() $(, $opt_args)*),
            DataType::Date32 => $macro!($self.date32().unwrap() $(, $opt_args)*),
            DataType::Date64 => $macro!($self.date64().unwrap() $(, $opt_args)*),
            _ => unimplemented!(),
        }
    }};
}

// doesn't include Bool and Utf8
#[macro_export]
macro_rules! apply_method_numeric_series {
    ($self:ident, $method:ident, $($args:expr),*) => {
        match $self.data_type() {
            DataType::UInt8 => $self.u8().unwrap().$method($($args),*),
            DataType::UInt16 => $self.u16().unwrap().$method($($args),*),
            DataType::UInt32 => $self.u32().unwrap().$method($($args),*),
            DataType::UInt64 => $self.u64().unwrap().$method($($args),*),
            DataType::Int8 => $self.i8().unwrap().$method($($args),*),
            DataType::Int16 => $self.i16().unwrap().$method($($args),*),
            DataType::Int32 => $self.i32().unwrap().$method($($args),*),
            DataType::Int64 => $self.i64().unwrap().$method($($args),*),
            DataType::Float32 => $self.f32().unwrap().$method($($args),*),
            DataType::Float64 => $self.f64().unwrap().$method($($args),*),
            DataType::Date32 => $self.date32().unwrap().$method($($args),*),
            DataType::Date64 => $self.date64().unwrap().$method($($args),*),

            _ => unimplemented!(),
        }
    }
}

#[macro_export]
macro_rules! match_data_type_apply_macro {
    ($obj:expr, $macro:ident, $macro_utf8:ident, $macro_bool:ident $(, $opt_args:expr)*) => {{
        match $obj {
            DataType::Utf8 => $macro_utf8!($($opt_args)*),
            DataType::Boolean => $macro_bool!($($opt_args)*),
            DataType::UInt8 => $macro!(UInt8Type $(, $opt_args)*),
            DataType::UInt16 => $macro!(UInt16Type $(, $opt_args)*),
            DataType::UInt32 => $macro!(UInt32Type $(, $opt_args)*),
            DataType::UInt64 => $macro!(UInt64Type $(, $opt_args)*),
            DataType::Int8 => $macro!(Int8Type $(, $opt_args)*),
            DataType::Int16 => $macro!(Int16Type $(, $opt_args)*),
            DataType::Int32 => $macro!(Int32Type $(, $opt_args)*),
            DataType::Int64 => $macro!(Int64Type $(, $opt_args)*),
            DataType::Float32 => $macro!(Float32Type $(, $opt_args)*),
            DataType::Float64 => $macro!(Float64Type $(, $opt_args)*),
            DataType::Date32 => $macro!(Date32Type $(, $opt_args)*),
            DataType::Date64 => $macro!(Date64Type $(, $opt_args)*),
            _ => unimplemented!(),
        }
    }};
}

macro_rules! format_data_value_with_option {
    ($F:expr, $EXPR:expr) => {{
        match $EXPR {
            Some(e) => write!($F, "{}", e),
            None => write!($F, "NULL"),
        }
    }};
}

macro_rules! typed_cast_from_data_value_to_std {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl DFTryFrom<DataValue> for $NATIVE {
            fn try_from(value: DataValue) -> Result<Self> {
                match value {
                    DataValue::$SCALAR(Some(inner_value)) => Ok(inner_value),
                    _ => Err(ErrorCode::BadDataValueType(format!(
                        "DataValue Error:  Cannot convert {:?} to {}",
                        value,
                        std::any::type_name::<Self>()
                    ))),
                }
            }
        }
    };
}

macro_rules! std_to_data_value {
    ($SCALAR:ident, $NATIVE:ident) => {
        impl From<$NATIVE> for DataValue {
            fn from(value: $NATIVE) -> Self {
                DataValue::$SCALAR(Some(value))
            }
        }

        impl From<Option<$NATIVE>> for DataValue {
            fn from(value: Option<$NATIVE>) -> Self {
                DataValue::$SCALAR(value)
            }
        }
    };
}

macro_rules! build_constant_series {
    ($ARRAY: ident, $VALUES: expr, $SIZE: expr) => {
        match $VALUES {
            Some(v) => $ARRAY::full(*v, $SIZE).into_series(),
            None => $ARRAY::full_null($SIZE).into_series(),
        }
    };
}

macro_rules! build_list_series {
    ($TYPE:ident,  $VALUES:expr, $SIZE:expr, $D_TYPE: expr) => {{
        type B = ListPrimitiveArrayBuilder<$TYPE>;
        let mut builder = B::with_capacity(0, $SIZE);
        match $VALUES {
            None => (0..$SIZE).for_each(|_| {
                builder.append_null();
            }),
            Some(v) => {
                let series = DataValue::try_into_data_array(&v, $D_TYPE)?;
                (0..$SIZE).for_each(|_| {
                    builder.append_series(&series);
                })
            }
        }
        Ok(builder.finish().into_series())
    }};
}

macro_rules! try_build_array {
    ($VALUE_BUILDER_TY:ident, $DF_TY:ident, $SCALAR_TY:ident, $VALUES:expr) => {{
        let mut builder = $VALUE_BUILDER_TY::<crate::$DF_TY>::with_capacity($VALUES.len());
        for value in $VALUES.iter() {
            match value {
                DataValue::$SCALAR_TY(Some(v)) => builder.append_value(*v),
                DataValue::$SCALAR_TY(None) => builder.append_null(),
                _ => unreachable!(),
            }
        }
        Ok(builder.finish().into_series())
    }};

    // Boolean
    ($VALUES:expr) => {{
        let mut builder = BooleanArrayBuilder::with_capacity($VALUES.len());
        for value in $VALUES.iter() {
            match value {
                DataValue::Boolean(Some(v)) => builder.append_value(*v),
                DataValue::Boolean(None) => builder.append_null(),
                _ => unreachable!(),
            }
        }
        Ok(builder.finish().into_series())
    }};

    // utf8
    ($utf8:ident, $VALUES:expr) => {{
        let mut builder = Utf8ArrayBuilder::with_capacity($VALUES.len());
        for value in $VALUES.iter() {
            match value {
                DataValue::Utf8(Some(v)) => builder.append_value(v),
                DataValue::Utf8(None) => builder.append_null(),
                _ => unreachable!(),
            }
        }
        Ok(builder.finish().into_series())
    }};
}

macro_rules! typed_data_value_min_max {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        Result::Ok(DataValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((*a).$OP(*b)),
        }))
    }};
}

// returns the sum of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_add {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        Result::Ok(DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) + (*b as $TYPE)),
        }))
    }};
}

// returns the sub of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_sub {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        Result::Ok(DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) - (*b as $TYPE)),
        }))
    }};
}

// returns the mul of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_mul {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        Result::Ok(DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) * (*b as $TYPE)),
        }))
    }};
}

// returns the div of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_div {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        Result::Ok(DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as f64),
            (None, Some(b)) => Some(b.clone() as f64),
            (Some(a), Some(b)) => Some((*a as f64) / (*b as f64)),
        }))
    }};
}

// returns the modulo of two data values, including coercion into $TYPE.
macro_rules! typed_data_value_modulo {
    ($OLD_VALUE:expr, $DELTA:expr, $SCALAR:ident, $TYPE:ident) => {{
        Result::Ok(DataValue::$SCALAR(match ($OLD_VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone() as $TYPE),
            (None, Some(b)) => Some(b.clone() as $TYPE),
            (Some(a), Some(b)) => Some((*a as $TYPE) % (*b as $TYPE)),
        }))
    }};
}

// min/max of two functions string values.
macro_rules! typed_data_value_min_max_string {
    ($VALUE:expr, $DELTA:expr, $SCALAR:ident, $OP:ident) => {{
        Result::Ok(DataValue::$SCALAR(match ($VALUE, $DELTA) {
            (None, None) => None,
            (Some(a), None) => Some(a.clone()),
            (None, Some(b)) => Some(b.clone()),
            (Some(a), Some(b)) => Some((a).$OP(b).clone()),
        }))
    }};
}
