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

use core::cmp::Ordering;
use core::fmt::Debug;
use core::fmt::{self};
use core::ops::Bound;
use core::ops::Range;
use std::collections::BTreeMap;

use super::range_wrapper::RangeWrapper;

#[derive(Clone)]
pub struct RangeMap<K, V> {
    // Wrap ranges so that they are `Ord`.
    // See `range_wrapper.rs` for explanation.
    pub(crate) btm: BTreeMap<RangeWrapper<K>, V>,
}

impl<K, V> Default for RangeMap<K, V>
where
    K: Ord + Clone + std::fmt::Debug + Copy,
    V: Eq + Clone + std::fmt::Debug,
{
    fn default() -> Self {
        Self::new()
    }
}

pub struct Iter<'a, K, V> {
    inner: std::collections::btree_map::Iter<'a, RangeWrapper<K>, V>,
}

impl<'a, K, V> Iterator for Iter<'a, K, V>
where
    K: 'a,
    V: 'a,
{
    type Item = (&'a Range<K>, &'a V);

    fn next(&mut self) -> Option<(&'a Range<K>, &'a V)> {
        self.inner.next().map(|(by_start, v)| (&by_start.range, v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<K: Debug, V: Debug> Debug for RangeMap<K, V>
where
    K: Ord + Clone + std::fmt::Debug + Copy,
    V: Eq + Clone + std::fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<K, V> RangeMap<K, V>
where
    K: Ord + Clone + std::fmt::Debug + Copy,
    V: Eq + Clone + std::fmt::Debug,
{
    /// Makes a new empty `RangeMap`.
    pub fn new() -> Self {
        RangeMap {
            btm: BTreeMap::new(),
        }
    }

    // Return a vector of (range,value) which contain the key in the [start, end).
    // If we have range [1,5],[2,4],[2,6], then:
    // 1. `get_by_key(1)` return [1,5]
    // 2. `get_by_key(2)` return [1,5],[2,4],[2,6]
    // 3. `get_by_key(5)` return [2,4],[2,6]
    pub fn get_by_key(&self, key: &K) -> Vec<(&RangeWrapper<K>, &V)> {
        let key_as_start = RangeWrapper::new(*key..*key);

        self.btm
            .range((Bound::Included(key_as_start.clone()), Bound::Unbounded))
            .filter(|e| {
                let start = e.0.range.start.cmp(&key_as_start.range.start);
                let end = e.0.range.end.cmp(&key_as_start.range.start);

                start != Ordering::Greater && end != Ordering::Less
            })
            .collect()
    }

    // Insert value into the range map,in case there is already the same key
    // , the old value will be overwrite.
    pub fn insert(&mut self, range: Range<K>, value: V) {
        assert!(range.start < range.end);

        let new_range_start_wrapper: RangeWrapper<K> = RangeWrapper::new(range);
        let new_value = value;

        // Insert the (possibly expanded) new range, and we're done!
        self.btm.insert(new_range_start_wrapper, new_value);
    }

    // Update or insert value into the range map.
    // If the key dose not exist, insert the new value directly;
    // otherwise, the new value if generated by the user define function
    // and replace the old value.
    pub fn upinsert<F>(&mut self, range: Range<K>, value: V, f: &mut F)
    where F: for<'a> FnMut(&'a mut V, &'a V) -> V {
        assert!(range.start < range.end);

        let new_range_start_wrapper: RangeWrapper<K> = RangeWrapper::new(range);
        let new_value = value;

        let old_value = self.btm.get_mut(&new_range_start_wrapper);
        match old_value {
            None => {
                self.btm.insert(new_range_start_wrapper, new_value);
            }
            Some(old_value) => {
                let new_value = f(old_value, &new_value);
                self.btm.insert(new_range_start_wrapper, new_value);
            }
        };
    }

    pub fn remove(&mut self, range: Range<K>) {
        self.btm.remove(&RangeWrapper::new(range));
    }

    pub fn get(&mut self, range: Range<K>) -> Option<&V> {
        self.btm.get(&RangeWrapper::new(range))
    }

    pub fn get_mut(&mut self, range: Range<K>) -> Option<&mut V> {
        self.btm.get_mut(&RangeWrapper::new(range))
    }

    /// Gets an iterator over all pairs of key range and value,
    /// ordered by key range.
    ///
    /// The iterator element type is `(&'a Range<K>, &'a V)`.
    pub fn iter(&self) -> Iter<'_, K, V> {
        Iter {
            inner: self.btm.iter(),
        }
    }
}
