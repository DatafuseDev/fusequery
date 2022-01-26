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

use std::any::Any;
use std::collections::BTreeMap;
use std::sync::Arc;

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_exception::Result;
use dyn_clone::DynClone;

use super::type_array::ArrayType;
use super::type_boolean::BooleanType;
use super::type_date::DateType;
use super::type_date32::Date32Type32;
use super::type_datetime::DateTimeType;
use super::type_datetime64::DateTime64Type;
use super::type_id::TypeID;
use super::type_nullable::NullableType;
use super::type_primitive::Float32Type;
use super::type_primitive::Float64Type;
use super::type_primitive::Int16Type;
use super::type_primitive::Int32Type;
use super::type_primitive::Int64Type;
use super::type_primitive::Int8Type;
use super::type_primitive::UInt16Type;
use super::type_primitive::UInt32Type;
use super::type_primitive::UInt64Type;
use super::type_primitive::UInt8Type;
use super::type_string::StringType;
use super::type_struct::StructType;
use crate::prelude::*;
use crate::TypeDeserializer;
use crate::TypeSerializer;

pub const ARROW_EXTENSION_NAME: &str = "ARROW:extension:databend_name";
pub const ARROW_EXTENSION_META: &str = "ARROW:extension:databend_metadata";

pub type DataTypePtr = Arc<dyn DataType>;

#[typetag::serde(tag = "type")]
pub trait DataType: std::fmt::Debug + Sync + Send + DynClone {
    fn data_type_id(&self) -> TypeID;

    fn is_nullable(&self) -> bool {
        false
    }

    fn as_any(&self) -> &dyn Any;

    fn default_value(&self) -> DataValue;

    fn create_constant_column(&self, data: &DataValue, size: usize) -> Result<ColumnRef>;

    fn create_column(&self, data: &[DataValue]) -> Result<ColumnRef>;

    /// arrow_type did not have nullable sign, it's nullable sign is in the field
    fn arrow_type(&self) -> ArrowType;

    fn custom_arrow_meta(&self) -> Option<BTreeMap<String, String>> {
        None
    }
    fn to_arrow_field(&self, name: &str) -> ArrowField {
        let ret = ArrowField::new(name, self.arrow_type(), self.is_nullable());
        if let Some(meta) = self.custom_arrow_meta() {
            ret.with_metadata(meta)
        } else {
            ret
        }
    }

    fn create_serializer(&self) -> Box<dyn TypeSerializer>;
    fn create_deserializer(&self, capacity: usize) -> Box<dyn TypeDeserializer>;
}

pub fn from_arrow_type(dt: &ArrowType) -> DataTypePtr {
    match dt {
        ArrowType::Null => Arc::new(NullableType::create(Arc::new(NullType {}))),
        ArrowType::UInt8 => Arc::new(UInt8Type::default()),
        ArrowType::UInt16 => Arc::new(UInt16Type::default()),
        ArrowType::UInt32 => Arc::new(UInt32Type::default()),
        ArrowType::UInt64 => Arc::new(UInt64Type::default()),
        ArrowType::Int8 => Arc::new(Int8Type::default()),
        ArrowType::Int16 => Arc::new(Int16Type::default()),
        ArrowType::Int32 => Arc::new(Int32Type::default()),
        ArrowType::Int64 => Arc::new(Int64Type::default()),
        ArrowType::Boolean => Arc::new(BooleanType::default()),
        ArrowType::Float32 => Arc::new(Float32Type::default()),
        ArrowType::Float64 => Arc::new(Float64Type::default()),

        // TODO support other list
        ArrowType::LargeList(f) => {
            let inner = from_arrow_field(f);
            Arc::new(ArrayType::create(inner))
        }

        ArrowType::Binary | ArrowType::LargeBinary | ArrowType::Utf8 | ArrowType::LargeUtf8 => {
            Arc::new(StringType::default())
        }

        ArrowType::Timestamp(_, tz) => Arc::new(DateTimeType::create(tz.clone())),
        ArrowType::Date32 => Arc::new(DateType::default()),
        ArrowType::Date64 => Arc::new(Date32Type32::default()),

        ArrowType::Struct(fields) => {
            let names = fields.iter().map(|f| f.name().to_string()).collect();
            let types = fields.iter().map(from_arrow_field).collect();

            Arc::new(StructType::create(names, types))
        }

        // this is safe, because we define the datatype firstly
        _ => {
            unimplemented!("data_type: {:?}", dt)
        }
    }
}

pub fn from_arrow_field(f: &ArrowField) -> DataTypePtr {
    let m = f.metadata();
    if let Some(custom_name) = m.get("ARROW:extension:databend_name") {
        let metatada = m.get("ARROW:extension:databend_metadata").cloned();
        match custom_name.as_str() {
            "Date" | "Date16" => return Arc::new(DateType::default()),
            "Date32" => return Arc::new(Date32Type32::default()),
            "DateTime" | "DateTime32" => return DateTimeType::arc(metatada),
            "DateTime64" => return DateTime64Type::arc(metatada),
            _ => {}
        }
    }

    let dt = f.data_type();
    let ty = from_arrow_type(dt);

    let is_nullable = f.is_nullable();
    if is_nullable {
        Arc::new(NullableType::create(ty))
    } else {
        ty
    }
}

pub fn wrap_nullable(data_type: &DataTypePtr) -> DataTypePtr {
    if matches!(data_type.data_type_id(), TypeID::Nullable | TypeID::Null) {
        return data_type.clone();
    }
    Arc::new(NullableType::create(data_type.clone()))
}

pub fn unwrap_nullable(data_type: &DataTypePtr) -> DataTypePtr {
    if matches!(data_type.data_type_id(), TypeID::Nullable) {
        let nullable = data_type.as_any().downcast_ref::<NullableType>().unwrap();
        return nullable.inner_type().clone();
    }
    data_type.clone()
}
