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

use std::collections::VecDeque;
use std::mem::MaybeUninit;
use std::slice::IterMut;

use crate::FastHash;
use crate::HashSet;
use crate::HashtableKeyable;
use crate::HashtableLike;
use crate::PartitionedHashSet;

pub struct PartitionedHashtable<Impl, const BUCKETS_LG2: u32 = 8> {
    tables: Vec<Impl>,
}

impl<Impl, const BUCKETS_LG2: u32> PartitionedHashtable<Impl, BUCKETS_LG2> {
    pub fn create(tables: Vec<Impl>) -> Self {
        assert_eq!(tables.len(), 1 << BUCKETS_LG2);
        PartitionedHashtable::<Impl, BUCKETS_LG2> { tables }
    }
}

impl<Impl: HashtableLike, const BUCKETS_LG2: u32> PartitionedHashtable<Impl, BUCKETS_LG2> {
    pub fn iter_tables_mut(&mut self) -> IterMut<'_, Impl> {
        self.tables.iter_mut()
    }
}

impl<K: HashtableKeyable + FastHash, const BUCKETS_LG2: u32> PartitionedHashSet<K, BUCKETS_LG2> {
    pub fn inner_sets_mut(&mut self) -> &mut Vec<HashSet<K>> {
        &mut self.tables
    }

    pub fn inner_sets(&self) -> &Vec<HashSet<K>> {
        &self.tables
    }

    pub fn set_merge(&mut self, other: &Self) {
        self.tables
            .iter_mut()
            .zip(other.tables.iter())
            .for_each(|(l, r)| {
                l.set_merge(r);
            });
    }

    pub fn set_insert(&mut self, key: &K) {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        let _ = unsafe { self.tables[index].insert_and_entry_with_hash(key, hash) };
    }
}

impl<K: ?Sized + FastHash, V, Impl: HashtableLike<Key = K, Value = V>, const BUCKETS_LG2: u32>
    HashtableLike for PartitionedHashtable<Impl, BUCKETS_LG2>
{
    type Key = Impl::Key;
    type Value = Impl::Value;
    type EntryRef<'a> = Impl::EntryRef<'a> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
    type EntryMutRef<'a> = Impl::EntryMutRef<'a> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
    type Iterator<'a> = PartitionedHashtableIter<Impl::Iterator<'a>> where Self: 'a, Self::Key: 'a, Self::Value: 'a;
    type IteratorMut<'a> = PartitionedHashtableIter<Impl::IteratorMut<'a>> where Self: 'a, Self::Key: 'a, Self::Value: 'a;

    fn len(&self) -> usize {
        self.tables.iter().map(|x| x.len()).sum::<usize>()
    }

    fn bytes_len(&self) -> usize {
        let mut impl_bytes = 0;
        for table in &self.tables {
            impl_bytes += table.bytes_len();
        }

        std::mem::size_of::<Self>() + impl_bytes
    }

    fn unsize_key_size(&self) -> Option<usize> {
        let mut key_len = 0;
        for table in &self.tables {
            key_len += table.unsize_key_size()?;
        }
        Some(key_len)
    }

    fn entry(&self, key: &Self::Key) -> Option<Self::EntryRef<'_>> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].entry(key)
    }

    fn entry_mut(&mut self, key: &Self::Key) -> Option<Self::EntryMutRef<'_>> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].entry_mut(key)
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].get(key)
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].get_mut(key)
    }

    unsafe fn insert(
        &mut self,
        key: &Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert(key)
    }

    unsafe fn insert_with_hash(
        &mut self,
        key_ref: &Self::Key,
        hash: u64,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert_with_hash(key_ref, hash)
    }

    #[inline(always)]
    unsafe fn insert_and_entry(
        &mut self,
        key: &Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let hash = key.fast_hash();
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert_and_entry_with_hash(key, hash)
    }

    #[inline(always)]
    unsafe fn insert_and_entry_with_hash(
        &mut self,
        key: &Self::Key,
        hash: u64,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let index = hash as usize >> (64u32 - BUCKETS_LG2);
        self.tables[index].insert_and_entry_with_hash(key, hash)
    }

    fn iter(&self) -> Self::Iterator<'_> {
        let mut inner = VecDeque::with_capacity(self.tables.len());
        for table in &self.tables {
            inner.push_back(table.iter());
        }

        PartitionedHashtableIter::create(inner)
    }

    fn clear(&mut self) {
        for inner_table in &mut self.tables {
            inner_table.clear();
        }
    }
}

pub struct PartitionedHashtableIter<Impl> {
    inner: VecDeque<Impl>,
}

impl<Impl> PartitionedHashtableIter<Impl> {
    pub fn create(inner: VecDeque<Impl>) -> Self {
        PartitionedHashtableIter::<Impl> { inner }
    }
}

impl<Impl: Iterator> Iterator for PartitionedHashtableIter<Impl> {
    type Item = Impl::Item;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.front_mut() {
                None => {
                    return None;
                }
                Some(front) => {
                    if let Some(next) = front.next() {
                        return Some(next);
                    }
                }
            }

            self.inner.pop_front();
        }
    }
}
