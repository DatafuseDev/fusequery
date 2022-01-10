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

use std::sync::Arc;

use common_arrow::arrow::array::Array;
use common_arrow::arrow::array::BinaryArray;
use common_arrow::arrow::bitmap::MutableBitmap;

use crate::arrays::mutable::MutableArrayBuilder;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DataType;

pub struct MutableStringArrayBuilder<const NULLABLE: bool> {
    data_type: DataType,
    offsets: Vec<i64>,
    values: Vec<u8>,
    validity: Option<MutableBitmap>,
}

impl<const NULLABLE: bool> MutableArrayBuilder for MutableStringArrayBuilder<NULLABLE> {
    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_mut_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn as_series(&mut self) -> Series {
        let array: Arc<dyn Array> = Arc::new(BinaryArray::from_data(
            self.data_type.to_arrow(),
            std::mem::take(&mut self.offsets).into(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ));
        array.into_series()
    }

    fn push_null(&mut self) {
        self.push_option::<&[u8]>(None);
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }
}

impl<const NULLABLE: bool> Default for MutableStringArrayBuilder<NULLABLE> {
    fn default() -> Self {
        Self::new()
    }
}

// for not nullable values
impl MutableStringArrayBuilder<false> {
    pub fn push<T: AsRef<[u8]>>(&mut self, value: T) {
        let bytes = value.as_ref();
        let size = (self.values.len() + bytes.len()) as i64;
        self.values.extend_from_slice(bytes);
        self.offsets.push(size);
    }
}

// for not nullable values
impl MutableStringArrayBuilder<true> {
    pub fn push<T: AsRef<[u8]>>(&mut self, value: T) {
        let bytes = value.as_ref();
        let size = (self.values.len() + bytes.len()) as i64;
        self.values.extend_from_slice(bytes);
        self.offsets.push(size);
        match &mut self.validity {
            Some(validity) => validity.push(true),
            None => {}
        }
    }
}

impl<const NULLABLE: bool> MutableStringArrayBuilder<NULLABLE> {
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut offsets = Vec::<i64>::with_capacity(capacity + 1);
        offsets.push(0i64);
        Self {
            data_type: DataType::String,
            offsets,
            values: Vec::<u8>::with_capacity(capacity),
            validity: None,
        }
    }

    pub fn from_data(offsets: Vec<i64>, values: Vec<u8>, validity: Option<MutableBitmap>) -> Self {
        Self {
            data_type: DataType::String,
            offsets,
            values,
            validity,
        }
    }

    pub fn push_option<T: AsRef<[u8]>>(&mut self, value: Option<T>) {
        match value {
            Some(value) => {
                let bytes = value.as_ref();
                let size = (self.values.len() + bytes.len()) as i64;
                self.values.extend_from_slice(bytes);
                self.offsets.push(size);
                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {}
                }
            }
            None => {
                self.offsets.push(self.last_offset());
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => self.init_validity(),
                }
            }
        }
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.offsets.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }

    #[inline]
    fn last_offset(&self) -> i64 {
        *self.offsets.last().unwrap()
    }

    fn len(&self) -> usize {
        self.offsets.len() - 1
    }
}
