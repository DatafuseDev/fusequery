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

use common_arrow::arrow::datatypes::DataType as ArrowType;

use super::data_type::DataType;
use super::type_id::TypeID;
use crate::prelude::*;

#[derive(Default, Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct StringType {}

impl StringType {
    pub fn new_impl() -> DataTypeImpl {
        DataTypeImpl::String(Self {})
    }
}

impl DataType for StringType {
    fn data_type_id(&self) -> TypeID {
        TypeID::String
    }

    fn name(&self) -> String {
        "String".to_string()
    }

    fn arrow_type(&self) -> ArrowType {
        ArrowType::LargeBinary
    }
}

impl std::fmt::Debug for StringType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
