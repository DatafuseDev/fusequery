// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::mem::size_of_val;

use async_raft::LogId;
pub use async_raft::NodeId;
use byteorder::BigEndian;
use byteorder::ByteOrder;
use common_exception::ErrorCode;
use common_metatypes::SeqValue;
use sled::IVec;

use crate::meta_service::sled_serde::SledOrderedSerde;
use crate::meta_service::SledSerde;

pub type LogIndex = u64;
pub type Term = u64;

/// NodeId, LogIndex and Term need to be serialized with order preserved, for listing items.
impl SledOrderedSerde for u64 {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        let size = size_of_val(self);
        let mut buf = vec![0; size];

        BigEndian::write_u64(&mut buf, *self);
        Ok(buf.into())
    }

    /// (de)serialize a value from `sled::IVec`.
    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        let res = BigEndian::read_u64(v.as_ref());
        Ok(res)
    }
}

/// For LogId to be able to stored in sled::Tree as a key.
impl SledOrderedSerde for String {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        Ok(IVec::from(self.as_str()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        Ok(String::from_utf8(v.as_ref().to_vec())?)
    }
}

impl SledSerde for String {
    fn ser(&self) -> Result<IVec, ErrorCode> {
        Ok(IVec::from(self.as_str()))
    }

    fn de<V: AsRef<[u8]>>(v: V) -> Result<Self, ErrorCode>
    where Self: Sized {
        Ok(String::from_utf8(v.as_ref().to_vec())?)
    }
}

impl SledSerde for SeqValue<Vec<u8>> {}

/// For LogId to be able to stored in sled::Tree as a value.
impl SledSerde for LogId {}
