// Copyright 2021 Datafuse Labs
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

use std::io::Cursor;
use std::io::Read;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_io::prelude::BinaryRead;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::MetaCompression;
use crate::meta::monotonically_increased_timestamp;
use crate::meta::trim_timestamp_to_micro_second;
use crate::meta::uuid_from_date_time;
use crate::meta::v4;
use crate::meta::ClusterKey;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::MetaEncoding;
use crate::meta::SnapshotId;
use crate::meta::Statistics;
use crate::meta::Versioned;

/// Compared to v4::TableSnapshot, the v5::TableSnapshot, a new field `least_visiable_timestamp` is added.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshot {
    /// format version of TableSnapshot meta data
    ///
    /// Note that:
    ///
    /// - A instance of v3::TableSnapshot may have a value of v2/v1::TableSnapshot::VERSION for this field.
    ///
    ///   That indicates this instance is converted from a v2/v1::TableSnapshot.
    ///
    /// - The meta writers are responsible for only writing down the latest version of TableSnapshot, and
    /// the format_version being written is of the latest version.
    ///
    ///   e.g. if the current version of TableSnapshot is v3::TableSnapshot, then the format_version
    ///   that will be written down to object storage as part of TableSnapshot table meta data,
    ///   should always be v3::TableSnapshot::VERSION (which is 3)
    pub format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,

    /// timestamp of this snapshot
    //  for backward compatibility, `Option` is used
    pub timestamp: Option<DateTime<Utc>>,

    // The table seq before snapshot commit.
    pub prev_table_seq: Option<u64>,

    /// previous snapshot
    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,

    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: TableSchema,

    /// Summary Statistics
    pub summary: Statistics,

    /// Pointers to SegmentInfos (may be of different format)
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,

    /// The metadata of the cluster keys.
    pub cluster_key_meta: Option<ClusterKey>,
    pub table_statistics_location: Option<String>,

    /// Some segments and blocks are generated in a transaction, the base snapshot is the latest snapshot committed before the transaction.
    ///
    /// If timestamp of the base snapshot is less than the `least_visiable_timestamp` of newly generated snapshot, the newly generated snapshot can't be committed.
    pub least_visiable_timestamp: Option<DateTime<Utc>>,
}

impl TableSnapshot {
    /// Note that base_snapshot_timestamp is not always equal to prev_timestamp.
    pub fn try_new(
        prev_table_seq: Option<u64>,
        prev_timestamp: &Option<DateTime<Utc>>,
        prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,
        prev_least_visiable_timestamp: &Option<DateTime<Utc>>,
        schema: TableSchema,
        summary: Statistics,
        segments: Vec<Location>,
        cluster_key_meta: Option<ClusterKey>,
        table_statistics_location: Option<String>,
        data_retention_time_in_days: u64,
        base_snapshot_timestamp: Option<DateTime<Utc>>,
    ) -> Result<Self> {
        let now = Utc::now();
        // make snapshot timestamp monotonically increased
        let adjusted_timestamp = monotonically_increased_timestamp(now, prev_timestamp);

        // trim timestamp to micro seconds
        let trimmed_timestamp = trim_timestamp_to_micro_second(adjusted_timestamp);
        let timestamp = Some(trimmed_timestamp);

        let candidate = trimmed_timestamp - Duration::days(data_retention_time_in_days as i64);
        let least_visiable_timestamp = match prev_least_visiable_timestamp {
            // UUIDv7 values are created by allocating a Unix timestamp in milliseconds in the most significant 48 bits
            // https://www.ietf.org/rfc/rfc9562.html#section-5.7
            Some(prev) => candidate.max(*prev + Duration::milliseconds(1)),
            None => candidate,
        };

        // when base_snapshot_timestamp.is_none(), it means no base snapshot or base snapshot has no timestamp,
        // both of them are allowed to be committed here.
        if base_snapshot_timestamp
            .as_ref()
            // safe to unwrap, least_visiable_timestamp of newly generated snapshot must be some
            .is_some_and(|base| base < &least_visiable_timestamp)
        {
            return Err(ErrorCode::TransactionTimeout(format!(
                "The timestamp of the base snapshot is: {:?}, the timestamp of the new snapshot is: {:?}",
                base_snapshot_timestamp.unwrap(),
                trimmed_timestamp
            )));
        }

        Ok(Self {
            format_version: TableSnapshot::VERSION,
            snapshot_id: uuid_from_date_time(trimmed_timestamp),
            timestamp,
            prev_table_seq,
            prev_snapshot_id,
            schema,
            summary,
            segments,
            cluster_key_meta,
            table_statistics_location,
            least_visiable_timestamp: Some(least_visiable_timestamp),
        })
    }

    pub fn new_empty_snapshot(schema: TableSchema, prev_table_seq: Option<u64>) -> Self {
        Self::try_new(
            prev_table_seq,
            &None,
            None,
            &None,
            schema,
            Statistics::default(),
            vec![],
            None,
            None,
            0,
            None,
        )
        .unwrap()
    }

    pub fn try_from_previous(
        previous: &TableSnapshot,
        prev_table_seq: Option<u64>,
        data_retention_time_in_days: u64,
        base_snapshot_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<Self> {
        let clone = previous.clone();
        // the timestamp of the new snapshot will be adjusted by the `new` method
        Self::try_new(
            prev_table_seq,
            &clone.timestamp,
            Some((clone.snapshot_id, clone.format_version)),
            &previous.least_visiable_timestamp,
            clone.schema,
            clone.summary,
            clone.segments,
            clone.cluster_key_meta,
            clone.table_statistics_location,
            data_retention_time_in_days,
            base_snapshot_timestamp,
        )
    }

    /// Serializes the struct to a byte vector.
    ///
    /// The byte vector contains the format version, encoding, compression, and compressed data. The encoding
    /// and compression are set to default values. The data is encoded and compressed.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized data as a byte vector. If any errors occur during
    /// encoding, compression, or writing to the byte vector, an error will be returned.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let encoding = MetaEncoding::MessagePack;
        let compression = MetaCompression::default();

        let data = encode(&encoding, &self)?;
        let data_compress = compress(&compression, data)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + data_compress.len().to_le_bytes().len()
            + data_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&data_compress.len().to_le_bytes());

        buf.extend(data_compress);

        Ok(buf)
    }

    /// Reads a snapshot from Vec<u8> and returns a `TableSnapshot` object.
    ///
    /// This function reads the following fields from the stream and constructs a `TableSnapshot` object:
    ///
    /// * `version` (u64): The version number of the snapshot.
    /// * `encoding` (u8): The encoding format used to serialize the snapshot's data.
    /// * `compression` (u8): The compression format used to compress the snapshot's data.
    /// * `snapshot_size` (u64): The size (in bytes) of the compressed snapshot data.
    ///
    /// The function then reads the compressed snapshot data from the stream, decompresses it using
    /// the specified compression format, and deserializes it using the specified encoding format.
    /// Finally, it constructs a `TableSnapshot` object using the deserialized data and returns it.
    pub fn from_slice(buffer: &[u8]) -> Result<TableSnapshot> {
        Self::from_read(Cursor::new(buffer))
    }

    pub fn from_read(mut r: impl Read) -> Result<TableSnapshot> {
        let version = r.read_scalar::<u64>()?;
        assert_eq!(version, TableSnapshot::VERSION);
        let encoding = MetaEncoding::try_from(r.read_scalar::<u8>()?)?;
        let compression = MetaCompression::try_from(r.read_scalar::<u8>()?)?;
        let snapshot_size: u64 = r.read_scalar::<u64>()?;

        read_and_deserialize(&mut r, snapshot_size, &encoding, &compression)
    }

    #[inline]
    pub fn encoding() -> MetaEncoding {
        MetaEncoding::MessagePack
    }
}

impl<T> From<T> for TableSnapshot
where T: Into<v4::TableSnapshot>
{
    fn from(s: T) -> Self {
        let s: v4::TableSnapshot = s.into();
        Self {
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of snapshot being converted.
            format_version: s.format_version,
            snapshot_id: s.snapshot_id,
            timestamp: s.timestamp,
            prev_table_seq: None,
            prev_snapshot_id: s.prev_snapshot_id,
            schema: s.schema,
            summary: s.summary,
            segments: s.segments,
            cluster_key_meta: s.cluster_key_meta,
            table_statistics_location: s.table_statistics_location,
            least_visiable_timestamp: None,
        }
    }
}

// A memory light version of TableSnapshot(Without segments)
// This *ONLY* used for some optimize operation, like PURGE/FUSE_SNAPSHOT function to avoid OOM.
#[derive(Clone, Debug)]
pub struct TableSnapshotLite {
    pub format_version: FormatVersion,
    pub snapshot_id: SnapshotId,
    pub timestamp: Option<DateTime<Utc>>,
    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,
    pub row_count: u64,
    pub block_count: u64,
    pub index_size: u64,
    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub segment_count: u64,
}

impl From<(&TableSnapshot, FormatVersion)> for TableSnapshotLite {
    fn from((value, ver): (&TableSnapshot, FormatVersion)) -> Self {
        TableSnapshotLite {
            format_version: ver,
            snapshot_id: value.snapshot_id,
            timestamp: value.timestamp,
            prev_snapshot_id: value.prev_snapshot_id,
            row_count: value.summary.row_count,
            block_count: value.summary.block_count,
            index_size: value.summary.index_size,
            uncompressed_byte_size: value.summary.uncompressed_byte_size,
            segment_count: value.segments.len() as u64,
            compressed_byte_size: value.summary.compressed_byte_size,
        }
    }
}