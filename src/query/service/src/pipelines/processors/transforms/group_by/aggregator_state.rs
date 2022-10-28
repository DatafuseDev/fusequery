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

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::intrinsics::likely;

use bumpalo::Bump;
use common_base::mem_allocator::GlobalAllocator;
use common_datablocks::HashMethod;
use common_datablocks::HashMethodFixedKeys;
use common_datablocks::HashMethodSerializer;
use common_datavalues::prelude::*;
use common_functions::aggregates::StateAddr;
use common_hashtable::HashMapKind;
use common_hashtable::HashMapKindIter;
use common_hashtable::HashtableEntry;
use common_hashtable::HashtableKeyable;
use common_hashtable::UnsizedHashMap;
use common_hashtable::UnsizedHashMapIter;
use common_hashtable::UnsizedHashtableEntryMutRef;
use common_hashtable::UnsizedHashtableEntryRef;

use crate::pipelines::processors::transforms::group_by::aggregator_state_entity::ShortFixedKeyable;
use crate::pipelines::processors::transforms::group_by::aggregator_state_entity::ShortFixedKeysStateEntity;
use crate::pipelines::processors::transforms::group_by::aggregator_state_entity::StateEntityMutRef;
use crate::pipelines::processors::transforms::group_by::aggregator_state_entity::StateEntityRef;
use crate::pipelines::processors::transforms::group_by::aggregator_state_iterator::ShortFixedKeysStateIterator;
use crate::pipelines::processors::AggregatorParams as NewAggregatorParams;

/// Aggregate state of the SELECT query, destroy when group by is completed.
///
/// It helps manage the following states:
///     - Aggregate data(HashMap or MergeSort set in future)
///     - Aggregate function state data memory pool
///     - Group by key data memory pool (if necessary)
#[allow(clippy::len_without_is_empty)]
pub trait AggregatorState<Method: HashMethod>: Sync + Send {
    type Key: ?Sized;
    type KeyRef<'a>: Copy + 'a
    where Self: 'a;
    type EntityRef<'a>: StateEntityRef<KeyRef = Self::KeyRef<'a>>
    where Self: 'a;
    type EntityMutRef<'a>: StateEntityMutRef<KeyRef = Self::KeyRef<'a>>
    where Self: 'a;
    type Iterator<'a>: Iterator<Item = Self::EntityRef<'a>>
    where Self: 'a;

    fn len(&self) -> usize;

    fn iter(&self) -> Self::Iterator<'_>;

    fn alloc_place(&self, layout: Layout) -> StateAddr;

    fn alloc_layout(&self, params: &NewAggregatorParams) -> Option<StateAddr> {
        params.layout?;
        let place: StateAddr = self.alloc_place(params.layout.unwrap());

        for idx in 0..params.offsets_aggregate_states.len() {
            let aggr_state = params.offsets_aggregate_states[idx];
            let aggr_state_place = place.next(aggr_state);
            params.aggregate_functions[idx].init_state(aggr_state_place);
        }
        Some(place)
    }

    fn entity(
        &mut self,
        key: Method::HashKeyRef<'_>,
        inserted: &mut bool,
    ) -> Self::EntityMutRef<'_>;

    fn entity_by_key<'a>(
        &mut self,
        key: Self::KeyRef<'a>,
        inserted: &mut bool,
    ) -> Self::EntityMutRef<'_>;

    fn is_two_level(&self) -> bool {
        false
    }

    fn convert_to_twolevel(&mut self) {}
}

/// The fixed length array is used as the data structure to locate the key by subscript
pub struct ShortFixedKeysAggregatorState<T: ShortFixedKeyable> {
    area: Bump,
    size: usize,
    max_size: usize,
    data: *mut ShortFixedKeysStateEntity<T>,
    two_level_flag: bool,
}

// TODO:(Winter) Hack:
// The *mut ShortFixedKeysStateEntity needs to be used externally, but we can ensure that *mut
// ShortFixedKeysStateEntity will not be used multiple async, so ShortFixedKeysAggregatorState is Send
unsafe impl<T: ShortFixedKeyable + Send> Send for ShortFixedKeysAggregatorState<T> {}

// TODO:(Winter) Hack:
// The *mut ShortFixedKeysStateEntity needs to be used externally, but we can ensure that &*mut
// ShortFixedKeysStateEntity will not be used multiple async, so ShortFixedKeysAggregatorState is Sync
unsafe impl<T: ShortFixedKeyable + Sync> Sync for ShortFixedKeysAggregatorState<T> {}

impl<T: ShortFixedKeyable> ShortFixedKeysAggregatorState<T> {
    pub fn create(max_size: usize) -> Self {
        unsafe {
            let size = max_size * std::mem::size_of::<ShortFixedKeysStateEntity<T>>();
            let entity_align = std::mem::align_of::<ShortFixedKeysStateEntity<T>>();
            let entity_layout = Layout::from_size_align_unchecked(size, entity_align);

            let raw_ptr = GlobalAllocator.alloc_zeroed(entity_layout);

            ShortFixedKeysAggregatorState::<T> {
                area: Default::default(),
                data: raw_ptr as *mut ShortFixedKeysStateEntity<T>,
                size: 0,
                max_size,
                two_level_flag: false,
            }
        }
    }
}

impl<T: ShortFixedKeyable> Drop for ShortFixedKeysAggregatorState<T> {
    fn drop(&mut self) {
        unsafe {
            let size = self.max_size * std::mem::size_of::<ShortFixedKeysStateEntity<T>>();
            let entity_align = std::mem::align_of::<ShortFixedKeysStateEntity<T>>();
            let layout = Layout::from_size_align_unchecked(size, entity_align);
            GlobalAllocator.dealloc(self.data as *mut u8, layout);
        }
    }
}

impl<T> AggregatorState<HashMethodFixedKeys<T>> for ShortFixedKeysAggregatorState<T>
where
    T: PrimitiveType + ShortFixedKeyable,
    for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey = T, HashKeyRef<'a> = T>,
    for<'a> <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashtableKeyable,
{
    type Key = T;
    type KeyRef<'a> = T;
    type EntityRef<'a> = &'a ShortFixedKeysStateEntity<T>;
    type EntityMutRef<'a> = &'a mut ShortFixedKeysStateEntity<T>;
    type Iterator<'a> = ShortFixedKeysStateIterator<'a, T>;

    #[inline(always)]
    fn len(&self) -> usize {
        self.size
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iterator<'_> {
        unsafe {
            let data = std::slice::from_raw_parts_mut(self.data, self.max_size);
            Self::Iterator::create(data)
        }
    }

    #[inline(always)]
    fn alloc_place(&self, layout: Layout) -> StateAddr {
        self.area.alloc_layout(layout).into()
    }

    #[inline(always)]
    fn entity(&mut self, key: T, inserted: &mut bool) -> Self::EntityMutRef<'_> {
        unsafe {
            let index = key.lookup();
            let value = self.data.offset(index);

            if likely((*value).fill) {
                *inserted = false;
                return &mut *(value);
            }

            *inserted = true;
            self.size += 1;
            (*value).key = key;
            (*value).fill = true;
            // It's an undefined behavior, but uninitialized integers rarely lead to problems.
            &mut (*value)
        }
    }

    #[inline(always)]
    fn entity_by_key<'a>(
        &mut self,
        key: Self::KeyRef<'a>,
        inserted: &mut bool,
    ) -> Self::EntityMutRef<'_> {
        self.entity(key, inserted)
    }

    #[inline(always)]
    fn is_two_level(&self) -> bool {
        self.two_level_flag
    }

    #[inline(always)]
    fn convert_to_twolevel(&mut self) {
        self.two_level_flag = true;
    }
}

pub struct LongerFixedKeysAggregatorState<T: HashtableKeyable> {
    pub area: Bump,
    pub data: HashMapKind<T, usize>,
    pub two_level_flag: bool,
}

impl<T: HashtableKeyable> Default for LongerFixedKeysAggregatorState<T> {
    fn default() -> Self {
        Self {
            area: Bump::new(),
            data: HashMapKind::new(),
            two_level_flag: false,
        }
    }
}

// TODO:(Winter) Hack:
// The *mut KeyValueEntity needs to be used externally, but we can ensure that *mut KeyValueEntity
// will not be used multiple async, so KeyValueEntity is Send
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl<T: HashtableKeyable + Send> Send for LongerFixedKeysAggregatorState<T> {}

// TODO:(Winter) Hack:
// The *mut KeyValueEntity needs to be used externally, but we can ensure that &*mut KeyValueEntity
// will not be used multiple async, so KeyValueEntity is Sync
unsafe impl<T: HashtableKeyable + Sync> Sync for LongerFixedKeysAggregatorState<T> {}

impl<T: Copy + Send + Sync + Sized + 'static> AggregatorState<HashMethodFixedKeys<T>>
    for LongerFixedKeysAggregatorState<T>
where
    for<'a> HashMethodFixedKeys<T>: HashMethod<HashKey = T, HashKeyRef<'a> = T>,
    for<'a> <HashMethodFixedKeys<T> as HashMethod>::HashKey: HashtableKeyable,
{
    type Key = T;
    type KeyRef<'a> = T;
    type EntityRef<'a> = &'a HashtableEntry<T, usize>;
    type EntityMutRef<'a> = &'a mut HashtableEntry<T, usize>;
    type Iterator<'a> = HashMapKindIter<'a, T, usize>;

    #[inline(always)]
    fn len(&self) -> usize {
        self.data.len()
    }

    #[inline(always)]
    fn iter(&self) -> Self::Iterator<'_> {
        self.data.iter()
    }

    #[inline(always)]
    fn alloc_place(&self, layout: Layout) -> StateAddr {
        self.area.alloc_layout(layout).into()
    }

    #[inline(always)]
    fn entity(&mut self, key: Self::Key, inserted: &mut bool) -> Self::EntityMutRef<'_> {
        match unsafe { self.data.insert_and_entry(key) } {
            Ok(e) => {
                *inserted = true;
                e
            }
            Err(e) => {
                *inserted = false;
                e
            }
        }
    }

    #[inline(always)]
    fn entity_by_key<'a>(
        &mut self,
        key: Self::KeyRef<'a>,
        inserted: &mut bool,
    ) -> Self::EntityMutRef<'_> {
        self.entity(key, inserted)
    }

    #[inline(always)]
    fn is_two_level(&self) -> bool {
        self.two_level_flag
    }

    #[inline(always)]
    fn convert_to_twolevel(&mut self) {
        self.data.convert_to_twolevel();
        self.two_level_flag = true;
    }
}

pub struct SerializedKeysAggregatorState {
    pub area: Bump,
    pub data_state_map: UnsizedHashMap<[u8], usize>,
    pub two_level_flag: bool,
}

// TODO:(Winter) Hack:
// The *mut KeyValueEntity needs to be used externally, but we can ensure that *mut KeyValueEntity
// will not be used multiple async, so KeyValueEntity is Send
#[allow(clippy::non_send_fields_in_send_ty)]
unsafe impl Send for SerializedKeysAggregatorState {}

// TODO:(Winter) Hack:
// The *mut KeyValueEntity needs to be used externally, but we can ensure that &*mut KeyValueEntity
// will not be used multiple async, so KeyValueEntity is Sync
unsafe impl Sync for SerializedKeysAggregatorState {}

impl AggregatorState<HashMethodSerializer> for SerializedKeysAggregatorState {
    type Key = [u8];
    type KeyRef<'a> = &'a [u8];
    type EntityRef<'a> = UnsizedHashtableEntryRef<'a, [u8], usize>;
    type EntityMutRef<'a> = UnsizedHashtableEntryMutRef<'a, [u8], usize>;
    type Iterator<'a> = UnsizedHashMapIter<'a, [u8], usize>;

    fn len(&self) -> usize {
        self.data_state_map.len()
    }
    fn iter(&self) -> Self::Iterator<'_> {
        self.data_state_map.iter()
    }

    #[inline(always)]
    fn alloc_place(&self, layout: Layout) -> StateAddr {
        self.area.alloc_layout(layout).into()
    }

    #[inline(always)]
    fn entity(&mut self, keys: &[u8], inserted: &mut bool) -> Self::EntityMutRef<'_> {
        unsafe {
            match self.data_state_map.insert_and_entry(keys) {
                Ok(e) => {
                    *inserted = true;
                    e
                }
                Err(e) => {
                    *inserted = false;
                    e
                }
            }
        }
    }

    #[inline(always)]
    fn entity_by_key<'a>(
        &mut self,
        key_ref: Self::KeyRef<'a>,
        inserted: &mut bool,
    ) -> Self::EntityMutRef<'_> {
        unsafe {
            match self.data_state_map.insert_and_entry(key_ref) {
                Ok(e) => {
                    *inserted = true;
                    e
                }
                Err(e) => {
                    *inserted = false;
                    e
                }
            }
        }
    }

    #[inline(always)]
    fn is_two_level(&self) -> bool {
        self.two_level_flag
    }

    #[inline(always)]
    fn convert_to_twolevel(&mut self) {
        // self.data_state_map.convert_to_twolevel();
        self.two_level_flag = true;
    }
}
