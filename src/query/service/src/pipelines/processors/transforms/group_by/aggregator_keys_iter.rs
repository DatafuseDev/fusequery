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

use common_datavalues::Column;
use common_datavalues::LargePrimitive;
use common_datavalues::PrimitiveColumn;
use common_datavalues::PrimitiveType;
use common_datavalues::ScalarColumn;
use common_datavalues::StringColumn;
use common_exception::Result;

pub trait KeysColumnIter<T> {
    fn get_slice(&self) -> &[T];
}

pub struct FixedKeysColumnIter<T>
where T: PrimitiveType
{
    pub inner: PrimitiveColumn<T>,
}

impl<T> FixedKeysColumnIter<T>
where T: PrimitiveType
{
    pub fn create(inner: &PrimitiveColumn<T>) -> Result<Self> {
        Ok(Self {
            inner: inner.clone(),
        })
    }
}

impl<T> KeysColumnIter<T> for FixedKeysColumnIter<T>
where T: PrimitiveType
{
    fn get_slice(&self) -> &[T] {
        self.inner.values()
    }
}

pub struct LargeFixedKeysColumnIter<T>
where T: LargePrimitive
{
    pub inner: Vec<T>,
}

impl<T> LargeFixedKeysColumnIter<T>
where T: LargePrimitive
{
    pub fn create(inner: &StringColumn) -> Result<Self> {
        let mut result = Vec::with_capacity(inner.len());
        for bs in inner.scalar_iter() {
            result.push(T::from_bytes(bs)?);
        }

        Ok(Self { inner: result })
    }
}

impl<T> KeysColumnIter<T> for LargeFixedKeysColumnIter<T>
where T: LargePrimitive
{
    fn get_slice(&self) -> &[T] {
        self.inner.as_slice()
    }
}

pub struct SerializedKeysColumnIter<'a> {
    inner: Vec<&'a [u8]>,
    #[allow(unused)]
    column: StringColumn,
}

impl<'a> SerializedKeysColumnIter<'a> {
    pub fn create(column: &'a StringColumn) -> Result<SerializedKeysColumnIter<'a>> {
        let values = column.values();
        let offsets = column.offsets();

        let mut inner = Vec::with_capacity(offsets.len() - 1);
        for index in 0..(offsets.len() - 1) {
            let offset = offsets[index] as usize;
            let offset_1 = offsets[index + 1] as usize;
            unsafe {
                let address = values.as_ptr().add(offset) as *const u8;
                inner.push(std::slice::from_raw_parts(address, offset_1 - offset));
            }
        }

        Ok(SerializedKeysColumnIter {
            inner,
            column: column.clone(),
        })
    }
}

impl<'a> KeysColumnIter<&'a [u8]> for SerializedKeysColumnIter<'a> {
    fn get_slice(&self) -> &[&'a [u8]] {
        self.inner.as_slice()
    }
}
