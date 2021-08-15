// Copyright 2020 Datafuse Labs.
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

use std::alloc::Layout;
use std::fmt;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DFTryFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::cast::AsPrimitive;
use num::NumCast;

use super::StateAddr;
use crate::aggregates::aggregator_common::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::dispatch_numeric_types;

// count = 0 means it's all nullable
// so we do not need option like sum
struct AggregateAvgState<T: BinarySer + BinaryDe> {
    pub value: T,
    pub count: u64,
}

impl<T> AggregateAvgState<T>
where T: std::ops::Add<Output = T> + Clone + Copy + BinarySer + BinaryDe
{
    #[inline(always)]
    fn add(&mut self, value: &Option<T>, count: u64) {
        if let Some(v) = value {
            self.value = self.value.add(*v);
            self.count += count;
        }
    }

    #[inline(always)]
    fn merge(&mut self, other: &Self) {
        self.value = self.value.add(other.value);
        self.count += other.count;
    }
}

#[derive(Clone)]
pub struct AggregateAvgFunction<T, SumT> {
    display_name: String,
    arguments: Vec<DataField>,
    t: PhantomData<T>,
    sum_t: PhantomData<SumT>,
}

impl<T, SumT> AggregateFunction for AggregateAvgFunction<T, SumT>
where
    T: DFNumericType,
    SumT: DFNumericType,
    T::Native: AsPrimitive<SumT::Native>
        + NumCast
        + DFTryFrom<DataValue>
        + Clone
        + Copy
        + Into<DataValue>
        + Send
        + Sync
        + 'static,
    SumT::Native: NumCast
        + DFTryFrom<DataValue>
        + Into<DataValue>
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = SumT::Native>
        + BinarySer
        + BinaryDe
        + Send
        + Sync
        + 'static,
    Option<SumT::Native>: Into<DataValue>,
{
    fn name(&self) -> &str {
        "AggregateAvgFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn nullable(&self, _input_schema: &DataSchema) -> Result<bool> {
        Ok(false)
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateAvgState::<SumT::Native> {
            value: SumT::Native::default(),
            count: 0,
        });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateAvgState<SumT::Native>>()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT::Native>>();
        let value = arrays[0].sum()?;
        let count = arrays[0].len() - arrays[0].null_count();
        let opt_sum: Option<SumT::Native> = DFTryFrom::try_from(value).ok();

        state.add(&opt_sum, count as u64);
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()> {
        let array: &DataArray<T> = arrays[0].static_cast();

        array.into_iter().zip(places.iter()).for_each(|(v, place)| {
            let place = place.next(offset);
            let state = place.get::<AggregateAvgState<SumT::Native>>();
            state.add(&v.map(|v| v.as_()), 1);
        });

        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT::Native>>();
        state.value.serialize_to_buf(writer)?;
        state.count.serialize_to_buf(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT::Native>>();
        state.value = SumT::Native::deserialize(reader)?;
        state.count = u64::deserialize(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateAvgState<SumT::Native>>();
        let rhs = rhs.get::<AggregateAvgState<SumT::Native>>();
        state.merge(rhs);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = place.get::<AggregateAvgState<SumT::Native>>();

        if state.count == 0 {
            return Ok(DataValue::Float64(None));
        }
        let v: f64 = NumCast::from(state.value).unwrap_or_default();
        Ok(DataValue::Float64(Some(v / state.count as f64)))
    }
}

impl<T, SumT> fmt::Display for AggregateAvgFunction<T, SumT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, SumT> AggregateAvgFunction<T, SumT>
where
    T: DFNumericType,
    SumT: DFNumericType,
    T::Native: AsPrimitive<SumT::Native>
        + NumCast
        + DFTryFrom<DataValue>
        + Clone
        + Copy
        + Into<DataValue>
        + Send
        + Sync
        + 'static,
    SumT::Native: NumCast
        + DFTryFrom<DataValue>
        + Into<DataValue>
        + Clone
        + Copy
        + Default
        + std::ops::Add<Output = SumT::Native>
        + BinarySer
        + BinaryDe
        + Send
        + Sync
        + 'static,
    Option<SumT::Native>: Into<DataValue>,
{
    pub fn try_create(
        display_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            arguments,
            t: PhantomData,
            sum_t: PhantomData,
        }))
    }
}

macro_rules! creator {
    ($T: ident, $data_type: expr, $display_name: expr, $arguments: expr) => {
        if $T::data_type() == $data_type {
            return AggregateAvgFunction::<$T, <$T as DFNumericType>::LargestType>::try_create(
                $display_name,
                $arguments,
            );
        }
    };
}

pub fn try_create_aggregate_avg_function(
    display_name: &str,
    arguments: Vec<DataField>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, arguments.len())?;

    let data_type = arguments[0].data_type();
    dispatch_numeric_types! {creator, data_type.clone(), display_name, arguments}

    Err(ErrorCode::BadDataValueType(format!(
        "AggregateSumFunction does not support type '{:?}'",
        data_type
    )))
}
