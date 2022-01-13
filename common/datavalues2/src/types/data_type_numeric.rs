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
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_exception::Result;

use super::data_type::IDataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Debug, Default, Clone, Copy, serde::Deserialize, serde::Serialize)]

pub struct DataTypeNumeric<
    T: PrimitiveType + Clone + Copy + std::fmt::Debug + Into<DataValue> + serde::Serialize,
> {
    _t: PhantomData<T>,
}

// typetag did not support generic impls, so we have to do this
pub fn create_primitive_datatype<T: PrimitiveType>() -> Arc<dyn IDataType> {
    match (T::SIGN, T::FLOATING, T::SIZE) {
        (false, false, 8) => Arc::new(DataTypeUInt8 { _t: PhantomData }),
        (false, false, 16) => Arc::new(DataTypeUInt16 { _t: PhantomData }),
        (false, false, 32) => Arc::new(DataTypeUInt32 { _t: PhantomData }),
        (false, false, 64) => Arc::new(DataTypeUInt64 { _t: PhantomData }),

        (false, false, 8) => Arc::new(DataTypeInt8 { _t: PhantomData }),
        (false, false, 16) => Arc::new(DataTypeInt16 { _t: PhantomData }),
        (false, false, 32) => Arc::new(DataTypeInt32 { _t: PhantomData }),
        (false, false, 64) => Arc::new(DataTypeInt64 { _t: PhantomData }),

        (true, true, 32) => Arc::new(DataTypeFloat32 { _t: PhantomData }),
        (true, true, 64) => Arc::new(DataTypeFloat64 { _t: PhantomData }),

        _ => unimplemented!(),
    }
}

pub type DataTypeInt8 = DataTypeNumeric<i8>;
pub type DataTypeInt16 = DataTypeNumeric<i16>;
pub type DataTypeInt32 = DataTypeNumeric<i32>;
pub type DataTypeInt64 = DataTypeNumeric<i64>;
pub type DataTypeUInt8 = DataTypeNumeric<u8>;
pub type DataTypeUInt16 = DataTypeNumeric<u16>;
pub type DataTypeUInt32 = DataTypeNumeric<u32>;
pub type DataTypeUInt64 = DataTypeNumeric<u64>;
pub type DataTypeFloat32 = DataTypeNumeric<f32>;
pub type DataTypeFloat64 = DataTypeNumeric<f64>;

macro_rules! impl_numeric {
    ($ty:ident, $tname:ident) => {
        impl DataTypeNumeric<$ty> {
            pub fn arc() -> DataTypePtr {
                Arc::new(Self { _t: PhantomData })
            }
        }

        #[typetag::serde]
        impl IDataType for DataTypeNumeric<$ty> {
            fn data_type_id(&self) -> TypeID {
                TypeID::$tname
            }

            #[inline]
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn default_value(&self) -> DataValue {
                $ty::default().into()
            }

            fn create_constant_column(
                &self,
                data: &DataValue,
                size: usize,
            ) -> common_exception::Result<ColumnRef> {
                let v: Result<$ty> = DFTryFrom::try_from(data);
                match v {
                    Ok(v) => Ok(PrimitiveColumn::<$ty>::full(v, size).into_column()),
                    _ => Ok(PrimitiveColumn::<$ty>::full_null(size).into_column()),
                }
            }

            fn arrow_type(&self) -> ArrowType {
                ArrowType::$tname
            }

            fn create_serializer(&self) -> Box<dyn TypeSerializer> {
                Box::new(NumberSerializer::<$ty>::default())
            }

            fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer> {
                Box::new(NumberDeserializer::<$ty> {
                    builder: MutablePrimitiveColumn::<$ty>::with_capacity(capacity),
                })
            }
        }
    };
}

impl_numeric!(u8, UInt8);
impl_numeric!(u16, UInt16);
impl_numeric!(u32, UInt32);
impl_numeric!(u64, UInt64);

impl_numeric!(i8, Int8);
impl_numeric!(i16, Int16);
impl_numeric!(i32, Int32);
impl_numeric!(i64, Int64);

impl_numeric!(f32, Float32);
impl_numeric!(f64, Float64);
