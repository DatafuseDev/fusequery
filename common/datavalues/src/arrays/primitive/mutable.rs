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
use common_arrow::arrow::array::PrimitiveArray;
use common_arrow::arrow::bitmap::MutableBitmap;

use crate::arrays::mutable::MutableArrayBuilder;
use crate::series::IntoSeries;
use crate::series::Series;
use crate::DFPrimitiveType;
use crate::DataType;

#[derive(Debug)]
pub struct MutablePrimitiveArrayBuilder<T, const NULLABLE: bool>
where T: DFPrimitiveType
{
    data_type: DataType,
    values: Vec<T>,
    validity: Option<MutableBitmap>,
}

impl<T, const NULLABLE: bool> MutableArrayBuilder for MutablePrimitiveArrayBuilder<T, NULLABLE>
where T: DFPrimitiveType
{
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
        let array: Arc<dyn Array> = Arc::new(PrimitiveArray::<T>::from_data(
            self.data_type.to_arrow(),
            std::mem::take(&mut self.values).into(),
            std::mem::take(&mut self.validity).map(|x| x.into()),
        ));
        array.into_series()
    }

    fn push_null(&mut self) {
        self.push_option(None)
    }

    fn validity(&self) -> Option<&MutableBitmap> {
        self.validity.as_ref()
    }
}

impl<T, const NULLABLE: bool> Default for MutablePrimitiveArrayBuilder<T, NULLABLE>
where T: DFPrimitiveType
{
    fn default() -> Self {
        Self::new()
    }
}

// for not nullable values
impl<T> MutablePrimitiveArrayBuilder<T, false>
where T: DFPrimitiveType
{
    pub fn push(&mut self, val: T) {
        self.values.push(val);
    }
}

// for nullable values
impl<T> MutablePrimitiveArrayBuilder<T, true>
where T: DFPrimitiveType
{
    pub fn push(&mut self, val: T) {
        self.values.push(val);
        match &mut self.validity {
            Some(validity) => validity.push(true),
            None => {}
        }
    }
}

impl<T, const NULLABLE: bool> MutablePrimitiveArrayBuilder<T, NULLABLE>
where T: DFPrimitiveType
{
    pub fn new() -> Self {
        Self::with_capacity(0)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        MutablePrimitiveArrayBuilder {
            data_type: T::data_type(),
            values: Vec::<T>::with_capacity(capacity),
            validity: None,
        }
    }

    pub fn from_data(data_type: DataType, values: Vec<T>, validity: Option<MutableBitmap>) -> Self {
        Self {
            data_type,
            values,
            validity,
        }
    }

    pub fn values(&self) -> &Vec<T> {
        &self.values
    }

    fn init_validity(&mut self) {
        let mut validity = MutableBitmap::with_capacity(self.values.capacity());
        validity.extend_constant(self.len(), true);
        validity.set(self.len() - 1, false);
        self.validity = Some(validity)
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    pub fn push_option(&mut self, val: Option<T>) {
        match val {
            Some(val) => {
                self.values.push(val);
                match &mut self.validity {
                    Some(validity) => validity.push(true),
                    None => {}
                }
            }
            None => {
                self.values.push(T::default());
                match &mut self.validity {
                    Some(validity) => validity.push(false),
                    None => {
                        self.init_validity();
                    }
                }
            }
        }
    }
}
