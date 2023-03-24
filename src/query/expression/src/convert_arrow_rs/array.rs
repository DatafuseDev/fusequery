// Copyright 2023 Datafuse Labs.
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

use arrow_array::make_array;
use arrow_array::Array;
use arrow_array::ArrowPrimitiveType;
use arrow_array::LargeStringArray;
use arrow_array::NullArray;
use arrow_array::PrimitiveArray;
use arrow_buffer::i256;
use arrow_buffer::ArrowNativeType;
use arrow_buffer::Buffer;
use arrow_data::ArrayData;
use arrow_data::ArrayDataBuilder;
use arrow_schema::ArrowError;
use arrow_schema::DataType;
use arrow_schema::TimeUnit;
use common_arrow::arrow::buffer::Buffer as Buffer2;
use common_arrow::arrow::Either;
use ordered_float::OrderedFloat;

use crate::types::decimal::DecimalColumn;
use crate::types::number::NumberColumn;
use crate::Column;

fn try_take_buffer<T: Clone>(buffer: Buffer2<T>) -> Vec<T> {
    // currently need a copy if buffer has more then 1 reference
    match buffer.into_mut() {
        Either::Left(b) => b.as_slice().to_vec(),
        Either::Right(v) => v,
    }
}

fn numbers_into<TN: ArrowNativeType, TA: ArrowPrimitiveType>(
    buf: Buffer2<TN>,
    data_type: DataType,
) -> Result<Arc<dyn Array>, ArrowError> {
    let v: Vec<TN> = try_take_buffer(buf);
    let len = v.len();
    let buf = Buffer::from_vec(v);
    let data = ArrayData::builder(data_type)
        .len(len)
        .offset(0)
        .add_buffer(buf)
        .build()?;
    Ok(Arc::new(PrimitiveArray::<TA>::from(data)))
}

impl Column {
    pub fn into_arrow_rs(self) -> Result<Arc<dyn Array>, ArrowError> {
        let array: Arc<dyn Array> = match self {
            Column::Null { len } => Arc::new(NullArray::new(len)),
            Column::EmptyArray { len } => Arc::new(NullArray::new(len)),
            Column::EmptyMap { len } => Arc::new(NullArray::new(len)),
            Column::String(col) => {
                let len = col.len();
                let values: Vec<u8> = try_take_buffer(col.data);
                let offsets: Vec<u64> = try_take_buffer(col.offsets);
                let offsets = Buffer::from_vec(offsets);
                let array_data = ArrayData::builder(DataType::LargeUtf8)
                    .len(len)
                    .add_buffer(offsets)
                    .add_buffer(values.into());
                let array_data = unsafe { array_data.build_unchecked() };
                Arc::new(LargeStringArray::from(array_data))
            }
            Column::Number(NumberColumn::UInt8(buf)) => {
                numbers_into::<u8, arrow_array::types::UInt8Type>(buf, DataType::UInt8)?
            }
            Column::Number(NumberColumn::UInt16(buf)) => {
                numbers_into::<u16, arrow_array::types::UInt16Type>(buf, DataType::UInt16)?
            }
            Column::Number(NumberColumn::UInt32(buf)) => {
                numbers_into::<u32, arrow_array::types::UInt32Type>(buf, DataType::UInt32)?
            }
            Column::Number(NumberColumn::UInt64(buf)) => {
                numbers_into::<u64, arrow_array::types::UInt64Type>(buf, DataType::UInt64)?
            }
            Column::Number(NumberColumn::Int8(buf)) => {
                numbers_into::<i8, arrow_array::types::Int8Type>(buf, DataType::Int8)?
            }
            Column::Number(NumberColumn::Int16(buf)) => {
                numbers_into::<i16, arrow_array::types::Int16Type>(buf, DataType::Int16)?
            }
            Column::Number(NumberColumn::Int32(buf)) => {
                numbers_into::<i32, arrow_array::types::Int32Type>(buf, DataType::Int32)?
            }
            Column::Number(NumberColumn::Int64(buf)) => {
                numbers_into::<i64, arrow_array::types::Int64Type>(buf, DataType::Int64)?
            }
            Column::Number(NumberColumn::Float32(buf)) => {
                let buf =
                    unsafe { std::mem::transmute::<Buffer2<OrderedFloat<f32>>, Buffer2<f32>>(buf) };
                numbers_into::<f32, arrow_array::types::Float32Type>(buf, DataType::Float32)?
            }
            Column::Number(NumberColumn::Float64(buf)) => {
                let buf =
                    unsafe { std::mem::transmute::<Buffer2<OrderedFloat<f64>>, Buffer2<f64>>(buf) };
                numbers_into::<f64, arrow_array::types::Float64Type>(buf, DataType::Float64)?
            }
            Column::Decimal(DecimalColumn::Decimal128(buf, size)) => {
                numbers_into::<i128, arrow_array::types::Decimal128Type>(
                    buf,
                    DataType::Decimal128(size.precision, size.scale as i8),
                )?
            }
            Column::Decimal(DecimalColumn::Decimal256(buf, size)) => {
                let v: Vec<ethnum::i256> = try_take_buffer(buf);
                // todo(youngsofun): arrow_rs use u128 for lo while arrow2 use i128, recheck it later.
                let v: Vec<i256> = v
                    .into_iter()
                    .map(|i| {
                        let (hi, lo) = i.into_words();
                        i256::from_parts(lo as u128, hi)
                    })
                    .collect();
                let len = v.len();
                let buf = Buffer::from_vec(v);
                let data =
                    ArrayData::builder(DataType::Decimal256(size.precision, size.scale as i8))
                        .len(len)
                        .add_buffer(buf)
                        .build()?;
                Arc::new(PrimitiveArray::<arrow_array::types::Decimal256Type>::from(
                    data,
                ))
            }

            Column::Timestamp(buf) => {
                numbers_into::<i64, arrow_array::types::Time64NanosecondType>(
                    buf,
                    DataType::Timestamp(TimeUnit::Nanosecond, None),
                )?
            }
            Column::Date(buf) => {
                numbers_into::<i32, arrow_array::types::Date32Type>(buf, DataType::Date32)?
            }
            Column::Nullable(col) => {
                let arrow_array = col.column.into_arrow_rs()?;
                let data = arrow_array.into_data();
                let buf = col.validity.as_slice().0;
                let builder = ArrayDataBuilder::from(data);
                // bitmap copied here
                let data = builder.null_bit_buffer(Some(buf.into())).build()?;
                make_array(data)
            }
            _ => {
                let data_type = self.data_type();
                Err(ArrowError::NotYetImplemented(format!(
                    "Column::into_arrow_rs()  for {data_type} not implemented yet"
                )))?
            }
        };
        Ok(array)
    }
}
