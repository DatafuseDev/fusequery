// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

use common_arrow::arrow::array as arrow_array;
use common_arrow::arrow::array::*;
use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::compute::aggregate;
use common_arrow::arrow::trusted_len::TrustedLen;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::prelude::*;
use crate::DataValue;

/// DataArray is generic struct which implements DataArray
pub struct DataArray<T> {
    pub array: arrow_array::ArrayRef,
    t: PhantomData<T>,
}

impl<T> DataArray<T> {
    pub fn new(array: arrow_array::ArrayRef) -> Self {
        Self {
            array,
            t: PhantomData::<T>,
        }
    }

    #[inline]
    pub fn data_type(&self) -> DataType {
        DataType::try_from(self.array.data_type()).unwrap()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.array.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn is_null(&self, row: usize) -> bool {
        self.array.is_null(row)
    }

    #[inline]
    pub fn null_count(&self) -> usize {
        self.array.null_count()
    }

    #[inline]
    pub fn all_is_null(&self) -> bool {
        self.null_count() == self.len()
    }

    #[inline]
    pub fn get_array_ref(&self) -> ArrayRef {
        self.array.clone()
    }

    #[inline]
    /// Get the null count and the buffer of bits representing null values
    pub fn null_bits(&self) -> (usize, &Option<Bitmap>) {
        (self.array.null_count(), self.array.validity())
    }

    pub fn limit(&self, num_elements: usize) -> Self {
        self.slice(0, num_elements)
    }

    pub fn get_array_memory_size(&self) -> usize {
        aggregate::estimated_bytes_size(self.array.as_ref())
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let array = Arc::from(self.array.slice(offset, length));
        Self::from(array)
    }

    /// Unpack a array to the same physical type.
    ///
    /// # Safety
    ///
    /// This is unsafe as the data_type may be uncorrect and
    /// is assumed to be correct in other unsafe code.
    pub unsafe fn unpack_array_matching_physical_type(
        &self,
        array: &Series,
    ) -> Result<&DataArray<T>> {
        let array_trait = &**array;
        if self.data_type() == array.data_type() {
            let ca = &*(array_trait as *const dyn SeriesTrait as *const DataArray<T>);
            Ok(ca)
        } else {
            use DataType::*;
            match (self.data_type(), array.data_type()) {
                (Int64, Date64) | (Int32, Date32) => {
                    let ca = &*(array_trait as *const dyn SeriesTrait as *const DataArray<T>);
                    Ok(ca)
                }
                _ => Err(ErrorCode::IllegalDataType(format!(
                    "cannot unpack array {:?} into matching type {:?}",
                    array,
                    self.data_type()
                ))),
            }
        }
    }
}

pub type DFNullArray = DataArray<NullType>;
pub type DFInt8Array = DataArray<Int8Type>;
pub type DFUInt8Array = DataArray<UInt8Type>;
pub type DFInt16Array = DataArray<Int16Type>;
pub type DFUInt16Array = DataArray<UInt16Type>;
pub type DFInt32Array = DataArray<Int32Type>;
pub type DFUInt32Array = DataArray<UInt32Type>;
pub type DFInt64Array = DataArray<Int64Type>;
pub type DFUInt64Array = DataArray<UInt64Type>;

pub type DFBooleanArray = DataArray<BooleanType>;

pub type DFFloat32Array = DataArray<Float32Type>;
pub type DFFloat64Array = DataArray<Float64Type>;

pub type DFUtf8Array = DataArray<Utf8Type>;
pub type DFListArray = DataArray<ListType>;
pub type DFStructArray = DataArray<StructType>;
pub type DFBinaryArray = DataArray<BinaryType>;

pub type DFDate32Array = DataArray<Date32Type>;
pub type DFDate64Array = DataArray<Date64Type>;

pub type DFTimestampSecondArray = DataArray<TimestampSecondType>;
pub type DFTimestampMillisecondArray = DataArray<TimestampMillisecondType>;
pub type DFTimestampMicrosecondArray = DataArray<TimestampMicrosecondType>;
pub type DFTimestampNanosecondArray = DataArray<TimestampNanosecondType>;
pub type DFIntervalYearMonthArray = DataArray<IntervalYearMonthType>;
pub type DFIntervalDayTimeArray = DataArray<IntervalDayTimeType>;

impl<T> DataArray<T>
where T: DFDataType
{
    pub fn name(&self) -> String {
        format!("DataArray<{:?}>", T::data_type())
    }

    /// # Safety
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub unsafe fn try_get(&self, index: usize) -> Result<DataValue> {
        let arr = &*self.array;
        macro_rules! downcast_and_pack {
            ($CAST_TYPE:ident, $SCALAR: ident) => {{
                let array = &*(arr as *const dyn Array as *const $CAST_TYPE);

                Ok(DataValue::$SCALAR(match array.is_null(index) {
                    true => None,
                    false => Some(array.value_unchecked(index).into()),
                }))
            }};
        }

        // TODO: insert types
        match T::data_type() {
            DataType::Utf8 => downcast_and_pack!(LargeUtf8Array, Utf8),
            DataType::Boolean => downcast_and_pack!(BooleanArray, Boolean),
            DataType::UInt8 => downcast_and_pack!(UInt8Array, UInt8),
            DataType::UInt16 => downcast_and_pack!(UInt16Array, UInt16),
            DataType::UInt32 => downcast_and_pack!(UInt32Array, UInt32),
            DataType::UInt64 => downcast_and_pack!(UInt64Array, UInt64),
            DataType::Int8 => downcast_and_pack!(Int8Array, Int8),
            DataType::Int16 => downcast_and_pack!(Int16Array, Int16),
            DataType::Int32 => downcast_and_pack!(Int32Array, Int32),
            DataType::Int64 => downcast_and_pack!(Int64Array, Int64),
            DataType::Float32 => downcast_and_pack!(Float32Array, Float32),
            DataType::Float64 => downcast_and_pack!(Float64Array, Float64),

            DataType::Binary => {
                downcast_and_pack!(LargeBinaryArray, Binary)
            }

            DataType::List(fs) => {
                let list_array = &*(arr as *const dyn Array as *const LargeListArray);
                let value = match list_array.is_null(index) {
                    true => None,
                    false => {
                        let nested_array: Arc<dyn Array> = Arc::from(list_array.value(index));
                        let series = nested_array.into_series();
                        let scalar_vec = (0..series.len())
                            .map(|i| series.try_get(i))
                            .collect::<Result<Vec<_>>>()?;

                        Some(scalar_vec)
                    }
                };
                Ok(DataValue::List(value, fs.data_type().clone()))
            }

            DataType::Struct(_) => {
                let struct_array = &*(arr as *const dyn Array as *const StructArray);
                let nested_array = struct_array.values()[index].clone();
                let series = nested_array.into_series();

                let scalar_vec = (0..series.len())
                    .map(|i| series.try_get(i))
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataValue::Struct(scalar_vec))
            }

            other => Result::Err(ErrorCode::BadDataValueType(format!(
                "DataValue Error: Can't create a functions of array of type \"{:?}\"",
                other
            ))),
        }
    }
}

impl<T> DataArray<T>
where T: DFPrimitiveType
{
    /// Create a new DataArray by taking ownership of the AlignedVec. This operation is zero copy.
    pub fn new_from_aligned_vec(values: AlignedVec<T::Native>) -> Self {
        let array = to_primitive::<T>(values, None);
        Self::new(Arc::new(array))
    }

    /// Nullify values in slice with an existing null bitmap
    pub fn new_from_owned_with_null_bitmap(
        values: AlignedVec<T::Native>,
        validity: Option<Bitmap>,
    ) -> Self {
        let array = to_primitive::<T>(values, validity);
        Self::new(Arc::new(array))
    }

    /// Get slices of the underlying arrow data.
    /// NOTE: null values should be taken into account by the user of these slices as they are handled
    /// separately

    pub fn data_views(
        &self,
    ) -> impl Iterator<Item = &T::Native> + '_ + Send + Sync + ExactSizeIterator + DoubleEndedIterator
    {
        self.downcast_ref().values().iter()
    }

    pub fn into_no_null_iter(
        &self,
    ) -> impl Iterator<Item = T::Native>
           + '_
           + Send
           + Sync
           + ExactSizeIterator
           + DoubleEndedIterator
           + TrustedLen {
        // .copied was significantly slower in benchmark, next call did not inline?
        self.data_views().copied().trust_my_length(self.len())
    }
}

impl DFListArray {
    pub fn sub_data_type(&self) -> DataType {
        match self.data_type() {
            DataType::List(sub_types) => sub_types.data_type().clone(),
            _ => unreachable!(),
        }
    }
}

impl<T> From<arrow_array::ArrayRef> for DataArray<T> {
    fn from(array: arrow_array::ArrayRef) -> Self {
        Self::new(array)
    }
}

impl<T> From<Box<dyn Array>> for DataArray<T> {
    fn from(array: Box<dyn Array>) -> Self {
        Self::new(Arc::from(array))
    }
}

impl<T> From<&arrow_array::ArrayRef> for DataArray<T> {
    fn from(array: &arrow_array::ArrayRef) -> Self {
        Self::new(array.clone())
    }
}

impl<T> Clone for DataArray<T> {
    fn clone(&self) -> Self {
        Self::new(self.array.clone())
    }
}

impl<T> std::fmt::Debug for DataArray<T>
where T: DFDataType
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DataArray<{:?}>", self.data_type())
    }
}

#[inline]
pub fn to_primitive<T: DFPrimitiveType>(
    values: AlignedVec<T::Native>,
    validity: Option<Bitmap>,
) -> PrimitiveArray<T::Native> {
    PrimitiveArray::from_data(T::data_type().to_arrow(), values.into(), validity)
}
