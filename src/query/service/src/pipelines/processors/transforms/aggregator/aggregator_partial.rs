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

use std::sync::Arc;

use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::HashMethod;
use common_functions::aggregates::StateAddr;
use common_functions::aggregates::StateAddrs;
use common_hashtable::HashtableEntryMutRefLike;
use common_hashtable::HashtableEntryRefLike;
use common_hashtable::HashtableLike;

use super::estimated_key_size;
use crate::pipelines::processors::transforms::group_by::Area;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::transform_aggregator::Aggregator;
use crate::pipelines::processors::AggregatorParams;

pub struct PartialAggregator<const HAS_AGG: bool, Method>
where Method: HashMethod + PolymorphicKeysHelper<Method>
{
    pub states_dropped: bool,

    pub area: Option<Area>,
    pub method: Method,
    pub hash_table: Method::HashTable,
    pub params: Arc<AggregatorParams>,
    pub input_rows: usize,
    pub generated: bool,
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send>
    PartialAggregator<HAS_AGG, Method>
{
    pub fn create(method: Method, params: Arc<AggregatorParams>) -> Result<Self> {
        let hash_table = method.create_hash_table()?;
        Ok(Self {
            params,
            method,
            hash_table,
            area: Some(Area::create()),
            states_dropped: false,
            input_rows: 0,
            generated: false,
        })
    }

    #[inline(always)]
    fn lookup_key(keys_iter: Method::HashKeyIter<'_>, hashtable: &mut Method::HashTable) {
        unsafe {
            for key in keys_iter {
                let _ = hashtable.insert_and_entry(key);
            }
        }
    }

    /// Allocate aggregation function state for each key(the same key can always get the same state)
    #[inline(always)]
    fn lookup_state(
        area: &mut Area,
        params: &Arc<AggregatorParams>,
        keys_iter: Method::HashKeyIter<'_>,
        hashtable: &mut Method::HashTable,
    ) -> StateAddrs {
        let mut places = Vec::with_capacity(keys_iter.size_hint().0);

        unsafe {
            for key in keys_iter {
                match hashtable.insert_and_entry(key) {
                    Ok(mut entry) => {
                        let place = params.alloc_layout(area);
                        places.push(place);
                        *entry.get_mut() = place.addr();
                    }
                    Err(entry) => {
                        let place = Into::<StateAddr>::into(*entry.get());
                        places.push(place);
                    }
                }
            }
        }

        places
    }

    // Block should be `convert_to_full`.
    #[inline(always)]
    fn aggregate_arguments(
        block: &DataBlock,
        params: &Arc<AggregatorParams>,
    ) -> Result<Vec<Vec<Column>>> {
        let aggregate_functions_arguments = &params.aggregate_functions_arguments;
        let mut aggregate_arguments_columns =
            Vec::with_capacity(aggregate_functions_arguments.len());
        for function_arguments in aggregate_functions_arguments {
            let mut function_arguments_column = Vec::with_capacity(function_arguments.len());

            for argument_index in function_arguments {
                // Unwrap safety: chunk has been `convert_to_full`.
                let argument_column = block
                    .get_by_offset(*argument_index)
                    .value
                    .as_column()
                    .unwrap();
                function_arguments_column.push(argument_column.clone());
            }

            aggregate_arguments_columns.push(function_arguments_column);
        }

        Ok(aggregate_arguments_columns)
    }

    pub fn do_streaming_consume(&mut self, block: DataBlock) -> Result<DataBlock> {
        let block = block.convert_to_full();
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&block, &self.params.group_columns);
        let group_columns = group_columns
            .iter()
            .map(|c| (c.value.as_column().unwrap().clone(), c.data_type.clone()))
            .collect::<Vec<_>>();

        let mut columns = Vec::with_capacity(self.params.aggregate_functions.len() + 1);

        if HAS_AGG {
            let area = self.area.as_mut().unwrap();
            let places = (0..block.num_rows())
                .map(|_| self.params.alloc_layout(area))
                .collect::<Vec<_>>();
            Self::execute(&self.params, &block, &places)?;

            let funcs = &self.params.aggregate_functions;
            let aggr_len = funcs.len();
            let offsets_aggregate_states = &self.params.offsets_aggregate_states;

            let rows = block.num_rows();
            // Builders.
            let mut state_builders = (0..aggr_len)
                .map(|_| StringColumnBuilder::with_capacity(rows, rows * 4))
                .collect::<Vec<_>>();

            // TODO batch
            for place in places {
                for (idx, func) in funcs.iter().enumerate() {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.serialize(arg_place, &mut state_builders[idx].data)?;

                    unsafe {
                        func.drop_state(arg_place);
                    }
                    state_builders[idx].commit_row();
                }
            }
            for builder in state_builders.into_iter() {
                columns.push(Column::String(builder.build()));
            }
        }
        let group_keys_state = self
            .method
            .build_keys_state(&group_columns, block.num_rows())?;
        let group_key = self.method.convert_state_to_column(group_keys_state)?;
        columns.push(group_key);
        Ok(DataBlock::new_from_columns(columns))
    }

    #[inline(always)]
    #[allow(clippy::ptr_arg)] // &[StateAddr] slower than &StateAddrs ~20%
    fn execute(
        params: &Arc<AggregatorParams>,
        block: &DataBlock,
        places: &StateAddrs,
    ) -> Result<()> {
        let aggregate_functions = &params.aggregate_functions;
        let offsets_aggregate_states = &params.offsets_aggregate_states;
        let aggregate_arguments_columns = Self::aggregate_arguments(block, params)?;

        // This can benificial for the case of dereferencing
        // This will help improve the performance ~hundreds of megabits per second
        let aggr_arg_columns_slice = &aggregate_arguments_columns;

        for index in 0..aggregate_functions.len() {
            let rows = block.num_rows();
            let function = &aggregate_functions[index];
            let state_offset = offsets_aggregate_states[index];
            let function_arguments = &aggr_arg_columns_slice[index];
            function.accumulate_keys(places, state_offset, function_arguments, rows)?;
        }

        Ok(())
    }

    #[inline(always)]
    pub fn group_columns<'a>(block: &'a DataBlock, indices: &[usize]) -> Vec<&'a BlockEntry> {
        indices
            .iter()
            .map(|&index| block.get_by_offset(index))
            .collect::<Vec<_>>()
    }

    #[inline(always)]
    fn generate_data(&mut self) -> Result<Vec<DataBlock>> {
        if self.generated || self.hash_table.len() == 0 {
            self.drop_states();
            return Ok(vec![]);
        }
        self.generated = true;

        let state_groups_len = self.hash_table.len();
        let aggregator_params = self.params.as_ref();
        let funcs = &aggregator_params.aggregate_functions;
        let aggr_len = funcs.len();
        let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

        // Builders.
        let mut state_builders = (0..aggr_len)
            .map(|_| StringColumnBuilder::with_capacity(state_groups_len, state_groups_len * 4))
            .collect::<Vec<_>>();

        let value_size = estimated_key_size(&self.hash_table);
        let mut group_key_builder = self
            .method
            .keys_column_builder(state_groups_len, value_size);
        for group_entity in self.hash_table.iter() {
            let place = Into::<StateAddr>::into(*group_entity.get());

            if HAS_AGG {
                for (idx, func) in funcs.iter().enumerate() {
                    let arg_place = place.next(offsets_aggregate_states[idx]);
                    func.serialize(arg_place, &mut state_builders[idx].data)?;
                    state_builders[idx].commit_row();
                }
            }
            group_key_builder.append_value(group_entity.key());
        }

        let mut columns = Vec::with_capacity(state_builders.len() + 1);

        if HAS_AGG {
            for builder in state_builders.into_iter() {
                columns.push(Column::String(builder.build()));
            }
        }

        let group_key_col = group_key_builder.finish();
        columns.push(group_key_col);
        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method> + Send> Aggregator
    for PartialAggregator<HAS_AGG, Method>
{
    const NAME: &'static str = "GroupByPartialTransform";

    fn consume(&mut self, block: DataBlock) -> Result<bool> {
        let block = block.convert_to_full();
        // 1.1 and 1.2.
        let group_columns = Self::group_columns(&block, &self.params.group_columns);
        let group_columns = group_columns
            .iter()
            .map(|c| (c.value.as_column().unwrap().clone(), c.data_type.clone()))
            .collect::<Vec<_>>();
        let group_keys_state = self
            .method
            .build_keys_state(&group_columns, block.num_rows())?;

        let group_keys_iter = self.method.build_keys_iter(&group_keys_state)?;

        if HAS_AGG {
            let area = self.area.as_mut().unwrap();
            let places =
                Self::lookup_state(area, &self.params, group_keys_iter, &mut self.hash_table);
            Self::execute(&self.params, &block, &places)?;
        } else {
            Self::lookup_key(group_keys_iter, &mut self.hash_table);
        }

        self.input_rows += block.num_rows();
        Ok(false)
    }

    fn generate(&mut self) -> Result<Vec<DataBlock>> {
        self.generate_data()
    }

    fn should_expand_table(&self) -> bool {
        /// ideas from https://github.com/apache/impala/blob/b3e9c4a65fa63da6f373c9ecec41fe4247e5e7d8/be/src/exec/grouping-aggregator.cc
        static STREAMING_HT_MIN_REDUCTION: [(usize, f64); 3] =
            [(0, 0.0), (256 * 1024, 1.1), (2 * 1024 * 1024, 2.0)];

        let ht_mem = self.hash_table.bytes_len();
        let ht_rows = self.hash_table.len();

        if ht_rows == 0 {
            return true;
        }

        let mut cache_level = 0;
        loop {
            if cache_level + 1 < STREAMING_HT_MIN_REDUCTION.len()
                && ht_mem >= STREAMING_HT_MIN_REDUCTION[cache_level + 1].0
            {
                cache_level += 1;
                continue;
            }
            break;
        }

        let aggregated_input_rows = self.input_rows;
        let current_reduction = aggregated_input_rows as f64 / ht_rows as f64;
        // TODO ADD estimated reduction, currently we use current reduction
        let estimated_reduction = current_reduction;
        let min_reduction = STREAMING_HT_MIN_REDUCTION[cache_level].1;

        println!(
            "YY: {:?} {:?} {}",
            estimated_reduction,
            min_reduction,
            estimated_reduction > min_reduction
        );
        estimated_reduction > min_reduction
    }

    /// don't need to find the key in hashtable, just initialize and update the State
    /// then flush into the output
    fn streaming_consume(&mut self, block: DataBlock) -> Result<DataBlock> {
        self.do_streaming_consume(block)
    }
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method>>
    PartialAggregator<HAS_AGG, Method>
{
    pub fn drop_states(&mut self) {
        if !self.states_dropped {
            let aggregator_params = self.params.as_ref();
            let aggregate_functions = &aggregator_params.aggregate_functions;
            let offsets_aggregate_states = &aggregator_params.offsets_aggregate_states;

            let functions = aggregate_functions
                .iter()
                .filter(|p| p.need_manual_drop_state())
                .collect::<Vec<_>>();

            let states = offsets_aggregate_states
                .iter()
                .enumerate()
                .filter(|(idx, _)| aggregate_functions[*idx].need_manual_drop_state())
                .map(|(_, s)| *s)
                .collect::<Vec<_>>();

            if !states.is_empty() {
                for group_entity in self.hash_table.iter() {
                    let place = Into::<StateAddr>::into(*group_entity.get());

                    for (function, state_offset) in functions.iter().zip(states.iter()) {
                        unsafe { function.drop_state(place.next(*state_offset)) }
                    }
                }
            }

            self.hash_table.clear();
            drop(self.area.take());
            self.states_dropped = true;
        }
    }
}

impl<const HAS_AGG: bool, Method: HashMethod + PolymorphicKeysHelper<Method>> Drop
    for PartialAggregator<HAS_AGG, Method>
{
    fn drop(&mut self) {
        self.drop_states();
    }
}
