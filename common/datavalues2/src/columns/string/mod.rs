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

mod iterator;
mod mutable;

use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::buffer::Buffer;
use common_arrow::arrow::compute::cast::binary_to_large_binary;
use common_arrow::arrow::datatypes::DataType as ArrowDataType;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::types::Index;
pub use iterator::*;
pub use mutable::*;

use crate::prelude::*;

// TODO adaptive offset
#[derive(Debug, Clone)]
pub struct StringColumn {
    offsets: Buffer<i64>,
    values: Buffer<u8>,
}

impl From<LargeBinaryArray> for StringColumn {
    fn from(array: LargeBinaryArray) -> Self {
        Self {
            offsets: array.offsets().clone(),
            values: array.values().clone(),
        }
    }
}

impl StringColumn {
    pub fn new(array: LargeBinaryArray) -> Self {
        Self {
            offsets: array.offsets().clone(),
            values: array.values().clone(),
        }
    }

    pub fn from_arrow_array(array: &dyn Array) -> Self {
        let arrow_type = array.data_type();
        if arrow_type == &ArrowDataType::Binary {
            let arr = array.as_any().downcast_ref::<BinaryArray<i32>>().unwrap();
            let arr = binary_to_large_binary(arr, ArrowDataType::LargeBinary);
            return Self::new(arr);
        }

        if arrow_type == &ArrowDataType::Utf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i32>>().unwrap();
            let iter = arr.values_iter();
            return Self::new_from_iter(iter);
        }

        if arrow_type == &ArrowDataType::LargeUtf8 {
            let arr = array.as_any().downcast_ref::<Utf8Array<i64>>().unwrap();
            let iter = arr.values_iter();
            return Self::new_from_iter(iter);
        }

        Self::new(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .clone(),
        )
    }

    /// construct StringColumn from unchecked data
    /// # Safety
    /// just like BinaryArray::from_data_unchecked, as follows
    /// * `offsets` MUST be monotonically increasing
    /// # Panics
    /// This function panics iff:
    /// * The last element of `offsets` is different from `values.len()`.
    /// * The validity is not `None` and its length is different from `offsets.len() - 1`.
    pub unsafe fn from_data_unchecked(offsets: Buffer<i64>, values: Buffer<u8>) -> Self {
        Self { offsets, values }
    }

    /// Returns the element at index `i`
    /// # Safety
    /// Assumes that the `i < self.len`.
    #[inline]
    pub unsafe fn value_unchecked(&self, i: usize) -> &[u8] {
        // soundness: the invariant of the function
        let start = self.offsets.get_unchecked(i).to_usize();
        let end = self.offsets.get_unchecked(i + 1).to_usize();
        // soundness: the invariant of the struct
        self.values.get_unchecked(start..end)
    }

    pub fn to_binary_array(&self) -> BinaryArray<i64> {
        unsafe {
            BinaryArray::from_data_unchecked(
                ArrowType::LargeBinary,
                self.offsets.clone(),
                self.values.clone(),
                None,
            )
        }
    }
}

impl Column for StringColumn {
    fn as_any(&self) -> &dyn std::any::Any {
        todo!()
    }

    fn data_type(&self) -> DataTypePtr {
        todo!()
    }

    fn is_nullable(&self) -> bool {
        todo!()
    }

    fn len(&self) -> usize {
        todo!()
    }

    fn null_at(&self, row: usize) -> bool {
        todo!()
    }

    fn validity(&self) -> (bool, Option<&Bitmap>) {
        todo!()
    }

    fn memory_size(&self) -> usize {
        todo!()
    }

    fn as_arrow_array(&self) -> ArrayRef {
        todo!()
    }

    fn slice(&self, offset: usize, length: usize) -> ColumnRef {
        todo!()
    }

    fn replicate(&self, offsets: &[usize]) -> ColumnRef {
        debug_assert!(
            offsets.len() == self.len(),
            "Size of offsets must match size of column"
        );

        if offsets.is_empty() {
            return self.slice(0, 0);
        }

        let max_size = offsets.iter().max().unwrap();
        let mut builder = MutableStringColumn::with_capacity(*max_size * 5, *max_size);

        let mut previous_offset: usize = 0;
        for i in 0..self.len() {
            let offset: usize = offsets[i];
            let data = unsafe { self.value_unchecked(i) };
            for _ in previous_offset..offset {
                builder.append_value(data);
            }
            previous_offset = offset;
        }
        builder.as_column()
    }

    unsafe fn get_unchecked(&self, index: usize) -> DataValue {
        todo!()
    }
}
