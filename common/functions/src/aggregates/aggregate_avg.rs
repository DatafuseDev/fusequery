// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::fmt;
use std::marker::PhantomData;

use common_datavalues::prelude::*;
use common_datavalues::DFTryFrom;
use common_exception::ErrorCode;
use common_exception::Result;
use common_io::prelude::*;
use num::NumCast;

use super::GetState;
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

impl<'a, T> GetState<'a, AggregateAvgState<T>> for AggregateAvgState<T> where T: BinarySer + BinaryDe
{}

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
    T::Native:
        NumCast + DFTryFrom<DataValue> + Clone + Copy + Into<DataValue> + Send + Sync + 'static,
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

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let state = arena.alloc(AggregateAvgState::<SumT::Native> {
            value: SumT::Native::default(),
            count: 0,
        });
        (state as *mut AggregateAvgState<SumT::Native>) as StateAddr
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        let state = AggregateAvgState::<SumT::Native>::get(place);
        let value = arrays[0].sum()?;
        let count = arrays[0].len() - arrays[0].null_count();
        let opt_sum: Option<SumT::Native> = DFTryFrom::try_from(value).ok();

        state.add(&opt_sum, count as u64);
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, arrays: &[Series]) -> Result<()> {
        let state = AggregateAvgState::<SumT::Native>::get(place);
        let value = arrays[0].try_get(row)?;

        let opt_sum: Option<T::Native> = DFTryFrom::try_from(value).ok();
        let opt_sum: Option<SumT::Native> = match opt_sum {
            Some(v) => NumCast::from(v),
            None => None,
        };

        state.add(&opt_sum, 1);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = AggregateAvgState::<SumT::Native>::get(place);
        state.value.serialize_to_buf(writer)?;
        state.count.serialize_to_buf(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = AggregateAvgState::<SumT::Native>::get(place);
        state.value = SumT::Native::deserialize(reader)?;
        state.count = u64::deserialize(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = AggregateAvgState::<SumT::Native>::get(place);
        let rhs = AggregateAvgState::<SumT::Native>::get(rhs);
        state.merge(rhs);
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateAvgState::<SumT::Native>::get(place);

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
    T::Native:
        NumCast + DFTryFrom<DataValue> + Clone + Copy + Into<DataValue> + Send + Sync + 'static,
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
