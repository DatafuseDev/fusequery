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

use metrics::counter;
use metrics::increment_gauge;

macro_rules! key {
    ($key: literal) => {
        concat!("fuse_", $key)
    };
}

pub fn metrics_inc_commit_mutation_unresolvable_conflict() {
    counter!(key!("commit_mutation_unresolvable_conflict"), 1);
}

pub fn metrics_inc_commit_mutation_latest_snapshot_append_only() {
    counter!(key!("commit_mutation_latest_snapshot_append_only"), 1);
}

pub fn metrics_inc_commit_mutation_modified_segment_exists_in_latest() {
    counter!(key!("modified_segment_exists_in_latest"), 1);
}

pub fn metrics_inc_commit_mutation_retry() {
    counter!(key!("commit_mutation_retry"), 1);
}

pub fn metrics_inc_commit_mutation_success() {
    counter!(key!("commit_mutation_success"), 1);
}

pub fn metrics_inc_commit_copied_files(n: usize) {
    counter!(key!("commit_copied_files"), n);
}

pub fn metrics_inc_commit_milliseconds(c: u128) {
    increment_gauge!(key!("commit_milliseconds"), c as f64);
}

pub fn metrics_inc_commit_aborts() {
    counter!(key!("commit_aborts"), 1);
}

pub fn metrics_inc_remote_io_seeks(c: u64) {
    increment_gauge!(key!("remote_io_seeks"), c as f64);
}

pub fn metrics_inc_remote_io_seeks_after_merged(c: u64) {
    increment_gauge!(key!("remote_io_seeks_after_merged"), c as f64);
}

pub fn metrics_inc_remote_io_read_bytes(c: u64) {
    increment_gauge!(key!("remote_io_read_bytes"), c as f64);
}

pub fn metrics_inc_remote_io_read_bytes_after_merged(c: u64) {
    increment_gauge!(key!("remote_io_read_bytes_after_merged"), c as f64);
}

pub fn metrics_inc_remote_io_read_parts(c: u64) {
    increment_gauge!(key!("remote_io_read_parts"), c as f64);
}

pub fn metrics_inc_remote_io_read_milliseconds(c: u64) {
    increment_gauge!(key!("remote_io_read_milliseconds"), c as f64);
}

pub fn metrics_inc_remote_io_deserialize_milliseconds(c: u64) {
    increment_gauge!(key!("remote_io_deserialize_milliseconds"), c as f64);
}

/// Block metrics.
pub fn metrics_inc_block_write_nums(c: u64) {
    increment_gauge!(key!("block_write_nums"), c as f64);
}

pub fn metrics_inc_block_write_bytes(c: u64) {
    increment_gauge!(key!("block_write_bytes"), c as f64);
}

pub fn metrics_inc_block_write_milliseconds(c: u64) {
    increment_gauge!(key!("block_write_milliseconds"), c as f64);
}

pub fn metrics_inc_block_index_write_nums(c: u64) {
    increment_gauge!(key!("block_index_write_nums"), c as f64);
}

pub fn metrics_inc_block_index_write_bytes(c: u64) {
    increment_gauge!(key!("block_index_write_bytes"), c as f64);
}

pub fn metrics_inc_block_index_write_milliseconds(c: u64) {
    increment_gauge!(key!("block_index_write_milliseconds"), c as f64);
}

pub fn metrics_inc_block_index_read_bytes(c: u64) {
    increment_gauge!(key!("block_index_read_bytes"), c as f64);
}

/// Compact metrics.
pub fn metrics_inc_compact_block_read_nums(c: u64) {
    increment_gauge!(key!("compact_block_read_nums"), c as f64);
}

pub fn metrics_inc_compact_block_read_bytes(c: u64) {
    increment_gauge!(key!("compact_block_read_bytes"), c as f64);
}

pub fn metrics_inc_compact_block_read_milliseconds(c: u64) {
    increment_gauge!(key!("compact_block_read_milliseconds"), c as f64);
}

/// Pruning metrics.
pub fn metrics_inc_segments_range_pruning_before(c: u64) {
    increment_gauge!(key!("segments_range_pruning_before"), c as f64);
}

pub fn metrics_inc_segments_range_pruning_after(c: u64) {
    increment_gauge!(key!("segments_range_pruning_after"), c as f64);
}

pub fn metrics_inc_bytes_segment_range_pruning_before(c: u64) {
    increment_gauge!(key!("bytes_segment_range_pruning_before"), c as f64);
}

pub fn metrics_inc_bytes_segment_range_pruning_after(c: u64) {
    increment_gauge!(key!("bytes_segment_range_pruning_after"), c as f64);
}

pub fn metrics_inc_blocks_range_pruning_before(c: u64) {
    increment_gauge!(key!("blocks_range_pruning_before"), c as f64);
}

pub fn metrics_inc_blocks_range_pruning_after(c: u64) {
    increment_gauge!(key!("blocks_range_pruning_after"), c as f64);
}

pub fn metrics_inc_bytes_block_range_pruning_before(c: u64) {
    increment_gauge!(key!("bytes_block_range_pruning_before"), c as f64);
}

pub fn metrics_inc_bytes_block_range_pruning_after(c: u64) {
    increment_gauge!(key!("bytes_block_range_pruning_after"), c as f64);
}

pub fn metrics_inc_blocks_bloom_pruning_before(c: u64) {
    increment_gauge!(key!("blocks_bloom_pruning_before"), c as f64);
}

pub fn metrics_inc_blocks_bloom_pruning_after(c: u64) {
    increment_gauge!(key!("blocks_bloom_pruning_after"), c as f64);
}

pub fn metrics_inc_bytes_block_bloom_pruning_before(c: u64) {
    increment_gauge!(key!("bytes_block_bloom_pruning_before"), c as f64);
}

pub fn metrics_inc_bytes_block_bloom_pruning_after(c: u64) {
    increment_gauge!(key!("bytes_block_bloom_pruning_after"), c as f64);
}

pub fn metrics_inc_pruning_prewhere_nums(c: u64) {
    increment_gauge!(key!("pruning_prewhere_nums"), c as f64);
}

pub fn metrics_inc_pruning_milliseconds(c: u64) {
    increment_gauge!(key!("pruning_milliseconds"), c as f64);
}

pub fn metrics_inc_deletion_block_range_pruned_nums(c: u64) {
    increment_gauge!(key!("deletion_block_range_pruned_nums"), c as f64);
}

pub fn metrics_inc_deletion_segment_range_purned_whole_segment_nums(c: u64) {
    increment_gauge!(
        key!("deletion_segment_range_pruned_whole_segment_nums"),
        c as f64
    );
}

pub fn metrics_inc_deletion_block_range_pruned_whole_block_nums(c: u64) {
    increment_gauge!(
        key!("deletion_block_range_pruned_whole_block_nums"),
        c as f64
    );
}
