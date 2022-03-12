// Copyright 2022 Datafuse Labs.
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
use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PartInfo;
use common_planners::PartInfoPtr;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct ColumnMeta {
    pub offset: u64,
    pub length: u64,
    pub num_values: u64,
}

impl ColumnMeta {
    pub fn create(offset: u64, length: u64, num_values: u64) -> ColumnMeta {
        ColumnMeta {
            offset,
            length,
            num_values,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct FusePartInfo {
    pub location: String,
    pub nums_rows: usize,
    pub columns_meta: HashMap<usize, ColumnMeta>,
}

#[typetag::serde(name = "fuse")]
impl PartInfo for FusePartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<FusePartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl FusePartInfo {
    pub fn create(
        location: String,
        rows_count: u64,
        columns_meta: HashMap<usize, ColumnMeta>,
    ) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(FusePartInfo {
            location,
            columns_meta,
            nums_rows: rows_count as usize,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&FusePartInfo> {
        match info.as_any().downcast_ref::<FusePartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::LogicalError(
                "Cannot downcast from PartInfo to FusePartInfo.",
            )),
        }
    }
}
