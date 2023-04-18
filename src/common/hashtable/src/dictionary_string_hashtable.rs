use std::fmt::Write;
use std::mem::MaybeUninit;
use std::ptr::NonNull;

use ahash::AHasher;
use bumpalo::Bump;

use crate::hashtable::Hashtable;
use crate::hashtable::HashtableIter;
use crate::hashtable::HashtableIterMut;
use crate::short_string_hashtable::ShortStringHashtable;
use crate::short_string_hashtable::ShortStringHashtableEntryMutRef;
use crate::string_hashtable::StringHashtable;
use crate::string_hashtable::StringHashtableEntryMutRef;
use crate::table0::Entry;
use crate::traits::Keyable;
use crate::traits::UnsizedKeyable;
use crate::DictionaryStringHashMap;
use crate::FastHash;
use crate::HashtableEntryMutRefLike;
use crate::HashtableEntryRefLike;
use crate::HashtableLike;
use crate::ShortStringHashSet;
use crate::StringHashSet;
use crate::StringHashtableEntryRef;

#[derive(Clone, Copy, Debug, Eq)]
pub struct DictionaryKeys {
    pub keys: NonNull<[NonNull<[u8]>]>,
    pub hash: u64,
}

impl PartialEq for DictionaryKeys {
    fn eq(&self, other: &Self) -> bool {
        if self.hash == other.hash {
            unsafe {
                return self.keys.as_ref() == other.keys.as_ref();
            }
        }

        false
    }
}

impl DictionaryKeys {
    pub fn create(keys: &[NonNull<[u8]>]) -> DictionaryKeys {
        unsafe {
            let hash = keys
                .iter()
                .map(|x| x.as_ref().fast_hash())
                .reduce(|left, right| {
                    let mut a = (left ^ right).wrapping_mul(0x9ddfea08eb382d69_u64);
                    a ^= a >> 47;
                    let mut b = (right ^ a).wrapping_mul(0x9ddfea08eb382d69_u64);
                    b ^= b >> 47;
                    b.wrapping_mul(0x9ddfea08eb382d69_u64)
                })
                .unwrap_or_default();

            DictionaryKeys {
                hash,
                keys: NonNull::from(keys),
            }
        }
    }

    pub fn create_with_hash(keys: &[NonNull<[u8]>], hash: u64) -> DictionaryKeys {
        DictionaryKeys {
            keys: NonNull::from(keys),
            hash,
        }
    }
}

unsafe impl Keyable for DictionaryKeys {
    #[inline(always)]
    fn is_zero(this: &MaybeUninit<Self>) -> bool {
        unsafe { this.assume_init_ref().keys.as_ref().is_empty() }
    }

    #[inline(always)]
    fn equals_zero(this: &Self) -> bool {
        unsafe { this.keys.as_ref().is_empty() }
    }

    #[inline(always)]
    fn hash(&self) -> u64 {
        self.hash
    }
}

impl FastHash for DictionaryKeys {
    fn fast_hash(&self) -> u64 {
        self.hash
    }
}

unsafe impl Send for DictionaryKeys {}

unsafe impl Sync for DictionaryKeys {}

pub struct DictionaryStringHashTable<V> {
    arena: Bump,
    hashtable: Hashtable<DictionaryKeys, V>,
    dictionary_hashset: StringHashSet<[u8]>,
}

unsafe impl<V> Send for DictionaryStringHashTable<V> {}

unsafe impl<V> Sync for DictionaryStringHashTable<V> {}

impl<V> DictionaryStringHashTable<V> {
    pub fn new() -> DictionaryStringHashMap<V> {
        DictionaryStringHashTable::<V> {
            arena: Bump::new(),
            hashtable: Hashtable::new(),
            dictionary_hashset: StringHashSet::new(),
        }
    }
}

impl<V> HashtableLike for DictionaryStringHashTable<V> {
    type Key = DictionaryKeys;
    type Value = V;
    type EntryRef<'a> = &'a Entry<DictionaryKeys, V> where Self: 'a, V: 'a;
    type EntryMutRef<'a> = &'a mut Entry<DictionaryKeys, V> where Self: 'a, V: 'a;
    type Iterator<'a> = HashtableIter<'a, DictionaryKeys, V> where Self: 'a, V: 'a;
    type IteratorMut<'a> = HashtableIterMut<'a, DictionaryKeys, V> where Self: 'a, V: 'a;

    fn len(&self) -> usize {
        self.hashtable.len()
    }

    fn bytes_len(&self) -> usize {
        self.dictionary_hashset.bytes_len()
            + self.hashtable.bytes_len()
            + self.arena.allocated_bytes()
    }

    fn entry(&self, key: &Self::Key) -> Option<Self::EntryRef<'_>> {
        let mut dictionary_keys = Vec::with_capacity(key.keys.len());

        unsafe {
            for key in key.keys.as_ref().iter() {
                let entry = self.dictionary_hashset.entry(key.as_ref())?;
                let s = NonNull::from(entry.key());
                dictionary_keys.push(s);
            }

            self.hashtable
                .entry(&DictionaryKeys::create(&dictionary_keys))
        }
    }

    fn entry_mut(&mut self, key: &Self::Key) -> Option<Self::EntryMutRef<'_>> {
        let mut dictionary_keys = Vec::with_capacity(key.keys.len());

        unsafe {
            for key in key.keys.as_ref().iter() {
                let entry = self.dictionary_hashset.entry(key.as_ref())?;
                dictionary_keys.push(NonNull::from(entry.key()));
            }

            self.hashtable
                .entry_mut(&DictionaryKeys::create(&dictionary_keys))
        }
    }

    fn get(&self, key: &Self::Key) -> Option<&Self::Value> {
        self.entry(key).map(|e| e.get())
    }

    fn get_mut(&mut self, key: &Self::Key) -> Option<&mut Self::Value> {
        self.entry_mut(key).map(|e| e.get_mut())
    }

    unsafe fn insert(
        &mut self,
        key: &Self::Key,
    ) -> Result<&mut MaybeUninit<Self::Value>, &mut Self::Value> {
        match self.insert_and_entry(key) {
            Err(e) => Err(e.get_mut()),
            Ok(e) => Ok(&mut *(e.val.as_ptr() as *mut V as *mut MaybeUninit<V>)),
        }
    }

    unsafe fn insert_and_entry(
        &mut self,
        key: &Self::Key,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let mut dictionary_keys = Vec::with_capacity(key.keys.len());

        for key in key.keys.as_ref().iter() {
            dictionary_keys.push(
                match self.dictionary_hashset.insert_and_entry(key.as_ref()) {
                    Ok(e) => NonNull::from(e.key()),
                    Err(e) => NonNull::from(e.key()),
                },
            );
        }

        let dictionary_key = DictionaryKeys::create(&dictionary_keys);
        let e = HashtableLike::insert_and_entry(&mut self.hashtable, &dictionary_key)?;
        e.set_key(DictionaryKeys::create(
            self.arena.alloc_slice_copy(&dictionary_keys),
        ));
        Ok(e)
    }

    unsafe fn insert_and_entry_with_hash(
        &mut self,
        key: &Self::Key,
        hash: u64,
    ) -> Result<Self::EntryMutRef<'_>, Self::EntryMutRef<'_>> {
        let mut dictionary_keys = Vec::with_capacity(key.keys.len());

        for key in key.keys.as_ref().iter() {
            dictionary_keys.push(
                match self.dictionary_hashset.insert_and_entry(key.as_ref()) {
                    Ok(e) => NonNull::from(e.key()),
                    Err(e) => NonNull::from(e.key()),
                },
            );
        }

        let dictionary_key = DictionaryKeys::create_with_hash(&dictionary_keys, hash);
        let e = HashtableLike::insert_and_entry(&mut self.hashtable, &dictionary_key)?;
        e.set_key(DictionaryKeys::create_with_hash(
            self.arena.alloc_slice_copy(&dictionary_keys),
            dictionary_key.hash,
        ));
        Ok(e)
    }

    fn iter(&self) -> Self::Iterator<'_> {
        self.hashtable.iter()
    }

    fn clear(&mut self) {
        self.hashtable.clear();
        self.dictionary_hashset.clear();
    }
}
