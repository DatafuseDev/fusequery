// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::RandomState;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::fmt;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;

use super::GetState;
use super::StateAddr;
use crate::aggregates::aggregate_function_factory::FactoryFunc;
use crate::aggregates::aggregator_common::assert_variadic_arguments;
use crate::aggregates::AggregateCountFunction;
use crate::aggregates::AggregateFunction;

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct DataGroupValues(Vec<DataGroupValue>);

pub struct AggregateDistinctState {
    set: HashSet<DataGroupValues, RandomState>,
    nested_addr: StateAddr,
}

impl AggregateDistinctState {
    pub fn serialize(&self, writer: &mut BytesMut) -> Result<()> {
        writer.write_uvarint(self.set.len() as u64)?;

        for entry in self.set.iter() {
            writer.write_uvarint(entry.0.len() as u64)?;
            for group_value in entry.0.iter() {
                let datavalue = DataValue::from(group_value);
                datavalue.serialize_to_buf(writer)?;
            }
        }
        Ok(())
    }

    pub fn deserialize(&mut self, reader: &mut &[u8]) -> Result<()> {
        self.set.clear();

        let size = reader.read_uvarint()?;
        self.set.reserve(size as usize);

        for _i in 0..size {
            let vsize = reader.read_uvarint()?;
            let mut values = Vec::with_capacity(vsize as usize);
            for _j in 0..vsize {
                let value = DataValue::deserialize(reader)?;
                let value = DataGroupValue::try_from(&value)?;
                values.push(value);
            }
            self.set.insert(DataGroupValues(values));
        }

        Ok(())
    }
}

impl<'a> GetState<'a, AggregateDistinctState> for AggregateDistinctState {}

#[derive(Clone)]
pub struct AggregateDistinctCombinator {
    name: String,

    nested_name: String,
    arguments: Vec<DataField>,
    nested: Arc<dyn AggregateFunction>,
}

impl AggregateDistinctCombinator {
    pub fn try_create_uniq(
        nested_name: &str,
        arguments: Vec<DataField>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        AggregateDistinctCombinator::try_create(
            nested_name,
            arguments,
            AggregateCountFunction::try_create,
        )
    }

    pub fn try_create(
        nested_name: &str,
        arguments: Vec<DataField>,
        nested_creator: FactoryFunc,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let name = format!("DistinctCombinator({})", nested_name);
        assert_variadic_arguments(&name, arguments.len(), (1, 32))?;

        let nested_arguments = match nested_name {
            "count" | "uniq" => vec![],
            _ => arguments.clone(),
        };

        let nested = nested_creator(nested_name, nested_arguments)?;
        Ok(Arc::new(AggregateDistinctCombinator {
            nested_name: nested_name.to_owned(),
            arguments,
            nested,
            name,
        }))
    }
}

impl AggregateFunction for AggregateDistinctCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.nested.nullable(input_schema)
    }

    fn allocate_state(&self, arena: &bumpalo::Bump) -> StateAddr {
        let addr = self.nested.allocate_state(arena);
        let state = arena.alloc(AggregateDistinctState {
            set: HashSet::new(),
            nested_addr: addr,
        });

        (state as *mut AggregateDistinctState) as StateAddr
    }

    fn accumulate_row(&self, place: StateAddr, row: usize, arrays: &[Series]) -> Result<()> {
        let state = AggregateDistinctState::get(place);

        let values = arrays
            .iter()
            .map(|c| c.try_get(row))
            .collect::<Result<Vec<_>>>()?;
        if !values.iter().any(|c| c.is_null()) {
            state.set.insert(DataGroupValues(
                values
                    .iter()
                    .map(DataGroupValue::try_from)
                    .collect::<Result<Vec<_>>>()?,
            ));
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        let state = AggregateDistinctState::get(place);
        state.serialize(writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = AggregateDistinctState::get(place);
        state.deserialize(reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = AggregateDistinctState::get(place);
        let rhs = AggregateDistinctState::get(rhs);

        state.set.extend(rhs.set.clone());
        Ok(())
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        let state = AggregateDistinctState::get(place);

        // faster path for count
        if self.nested.name() == "AggregateFunctionCount" {
            Ok(DataValue::UInt64(Some(state.set.len() as u64)))
        } else {
            if state.set.is_empty() {
                return self.nested.merge_result(state.nested_addr);
            }
            let mut results = Vec::with_capacity(state.set.len());

            state.set.iter().for_each(|group_values| {
                let mut v = Vec::with_capacity(group_values.0.len());
                group_values.0.iter().for_each(|group_value| {
                    v.push(DataValue::from(group_value));
                });

                results.push(v);
            });

            let results = (0..self.arguments.len())
                .map(|i| {
                    results
                        .iter()
                        .map(|inner| inner[i].clone())
                        .collect::<Vec<_>>()
                })
                .collect::<Vec<_>>();

            let arrays = results
                .iter()
                .enumerate()
                .map(|(i, v)| DataValue::try_into_data_array(v, self.arguments[i].data_type()))
                .collect::<Result<Vec<_>>>()?;

            self.nested
                .accumulate(state.nested_addr, &arrays, state.set.len())?;
            // merge_result
            self.nested.merge_result(state.nested_addr)
        }
    }
}

impl fmt::Display for AggregateDistinctCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested_name)
    }
}
