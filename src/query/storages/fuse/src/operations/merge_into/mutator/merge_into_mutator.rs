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

use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hasher;
use std::sync::Arc;

use common_arrow::arrow::bitmap::MutableBitmap;
use common_base::base::ProgressValues;
use common_base::runtime::GlobalIORuntime;
use common_catalog::plan::Projection;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_expression::TableSchema;
use opendal::Operator;
use siphasher::sip128;
use siphasher::sip128::Hasher128;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use tracing::info;

use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::WriteSettings;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::DeletionByColumn;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::MergeIntoOperation;
use crate::operations::merge_into::mutation_meta::merge_into_operation_meta::UniqueKeyDigest;
use crate::operations::merge_into::mutation_meta::BlockMetaIndex;
use crate::operations::merge_into::mutation_meta::MutationLogEntry;
use crate::operations::merge_into::mutation_meta::MutationLogs;
use crate::operations::merge_into::mutation_meta::Replacement;
use crate::operations::merge_into::mutation_meta::ReplacementLogEntry;
use crate::operations::merge_into::mutator::deletion_accumulator::DeletionAccumulator;
use crate::operations::merge_into::OnConflictField;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;

// Apply MergeIntoOperations to segments
pub struct MergeIntoOperationAggregator {
    segment_locations: HashMap<SegmentIndex, Location>,
    deletion_accumulator: DeletionAccumulator,
    on_conflict_fields: Vec<OnConflictField>,
    block_reader: Arc<BlockReader>,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    segment_reader: CompactSegmentInfoReader,
    block_builder: BlockBuilder,
}

impl MergeIntoOperationAggregator {
    #[allow(clippy::too_many_arguments)] // TODO fix this
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        on_conflict_fields: Vec<OnConflictField>,
        segment_locations: Vec<(SegmentIndex, Location)>,
        data_accessor: Operator,
        table_schema: Arc<TableSchema>,
        write_settings: WriteSettings,
        read_settings: ReadSettings,
        block_builder: BlockBuilder,
    ) -> Result<Self> {
        let deletion_accumulator = DeletionAccumulator::default();
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), table_schema.clone());
        let indices = (0..table_schema.fields().len()).collect::<Vec<usize>>();
        let projection = Projection::Columns(indices);
        let block_reader = BlockReader::create(
            data_accessor.clone(),
            table_schema,
            projection,
            ctx.clone(),
            false,
        )?;

        Ok(Self {
            segment_locations: HashMap::from_iter(segment_locations.into_iter()),
            deletion_accumulator,
            on_conflict_fields,
            block_reader,
            data_accessor,
            write_settings,
            read_settings,
            segment_reader,
            block_builder,
        })
    }
}

// aggregate mutations (currently, deletion only)
impl MergeIntoOperationAggregator {
    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, merge_action: MergeIntoOperation) -> Result<()> {
        match &merge_action {
            MergeIntoOperation::Delete(DeletionByColumn {
                columns_min_max,
                key_hashes,
            }) => {
                for (segment_index, (path, ver)) in &self.segment_locations {
                    let load_param = LoadParams {
                        location: path.clone(),
                        len_hint: None,
                        ver: *ver,
                        put_cache: true,
                    };
                    // for typical configuration, segment cache is enabled, thus after the first loop, we are reading from cache
                    let segment_info = self.segment_reader.read(&load_param).await?;
                    let segment_info: SegmentInfo = segment_info.as_ref().try_into()?;

                    // segment level
                    if self.overlapped(&segment_info.summary.col_stats, columns_min_max) {
                        // block level
                        for (block_index, block_meta) in segment_info.blocks.iter().enumerate() {
                            if self.overlapped(&block_meta.col_stats, columns_min_max) {
                                self.deletion_accumulator.add_block_deletion(
                                    *segment_index,
                                    block_index,
                                    key_hashes,
                                )
                            }
                        }
                    }
                }
            }
            MergeIntoOperation::None => {}
        }
        Ok(())
    }
}

// apply the mutations and generate mutation log
impl MergeIntoOperationAggregator {
    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<MutationLogs>> {
        let mut mutation_logs = Vec::new();
        for (segment_idx, block_deletion) in &self.deletion_accumulator.deletions {
            // do we need a local cache?
            let (path, ver) = self.segment_locations.get(segment_idx).ok_or_else(|| {
                ErrorCode::Internal(format!(
                    "unexpected, segment (idx {}) not found, during applying mutation log",
                    segment_idx
                ))
            })?;

            let load_param = LoadParams {
                location: path.clone(),
                len_hint: None,
                ver: *ver,
                put_cache: true,
            };

            let compact_segment_info = self.segment_reader.read(&load_param).await?;
            let segment_info: SegmentInfo = compact_segment_info.as_ref().try_into()?;

            for (block_index, keys) in block_deletion {
                let block_meta = &segment_info.blocks[*block_index];
                if let Some(segment_mutation_log) = self
                    .apply_deletion_to_data_block(*segment_idx, *block_index, block_meta, keys)
                    .await?
                {
                    mutation_logs.push(MutationLogEntry::Replacement(segment_mutation_log));
                }
            }
        }
        Ok(Some(MutationLogs {
            entries: mutation_logs,
        }))
    }

    #[async_backtrace::framed]
    async fn apply_deletion_to_data_block(
        &self,
        segment_index: SegmentIndex,
        block_index: BlockIndex,
        block_meta: &Arc<BlockMeta>,
        deleted_key_hashes: &HashSet<UniqueKeyDigest>,
    ) -> Result<Option<ReplacementLogEntry>> {
        info!(
            "apply delete to segment idx {}, block idx {}",
            segment_index, block_index
        );
        if block_meta.row_count == 0 {
            return Ok(None);
        }

        let reader = self.block_reader.clone();
        let on_conflict_fields = &self.on_conflict_fields;

        // load block data
        let columns_meta = &block_meta.col_metas;
        let block_path = &block_meta.location.0.clone();
        let merged_io_read_result = reader
            .read_columns_data_by_merge_io(&self.read_settings, block_path, columns_meta)
            .await?;

        // deserialize block data.
        let storage_format = self.write_settings.storage_format;
        let meta = block_meta.clone();
        // cpu intensive task, submit it to global io runtime as block task
        let data_block = GlobalIORuntime::instance()
            .spawn_blocking(move || {
                let column_chunks = merged_io_read_result.columns_chunks()?;
                reader.deserialize_chunks(
                    &meta.location.0,
                    meta.row_count as usize,
                    &meta.compression,
                    &meta.col_metas,
                    column_chunks,
                    &storage_format,
                )
            })
            .await?;

        let num_rows = data_block.num_rows();

        let mut columns = Vec::with_capacity(on_conflict_fields.len());
        for field in on_conflict_fields {
            let on_conflict_field_index = field.field_index;
            let key_column = data_block
                .columns()
                .get(on_conflict_field_index)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, block entry (index {}) not found. segment index {}, block index {}",
                        on_conflict_field_index, segment_index, block_index
                    ))
                })?
                .value
                .as_column()
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, cast block entry (index {}) to column failed, got None. segment index {}, block index {}",
                        on_conflict_field_index, segment_index, block_index
                    ))
                })?;
            columns.push(key_column);
        }

        let mut bitmap = MutableBitmap::new();
        for row in 0..num_rows {
            let mut sip = sip128::SipHasher24::new();
            for column in &columns {
                let value = column.index(row).unwrap();
                let string = value.to_string();
                sip.write(string.as_bytes());
            }
            let hash = sip.finish128().as_u128();
            bitmap.push(!deleted_key_hashes.contains(&hash));
        }

        let delete_nums = bitmap.unset_bits();
        // shortcuts
        if delete_nums == 0 {
            info!("nothing deleted");
            // nothing to be deleted
            return Ok(None);
        }

        let progress_values = ProgressValues {
            rows: delete_nums,
            // ignore bytes.
            bytes: 0,
        };
        self.block_builder
            .ctx
            .get_write_progress()
            .incr(&progress_values);

        if delete_nums == block_meta.row_count as usize {
            info!("whole block deletion");
            // whole block deletion
            let mutation = ReplacementLogEntry {
                index: BlockMetaIndex {
                    segment_idx: segment_index,
                    block_idx: block_index,
                },
                op: Replacement::Deleted,
            };

            return Ok(Some(mutation));
        }

        let bitmap = bitmap.into();
        let new_block = data_block.filter_with_bitmap(&bitmap)?;
        info!("number of row deleted: {}", delete_nums);

        // serialization and compression is cpu intensive, send them to dedicated thread pool
        // and wait (asyncly, which will NOT block the executor thread)
        let block_builder = self.block_builder.clone();
        let origin_stats = block_meta.cluster_stats.clone();
        let serialized = GlobalIORuntime::instance()
            .spawn_blocking(move || {
                block_builder.build(new_block, |block, generator| {
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    Ok((cluster_stats, block))
                })
            })
            .await?;

        // persistent data
        let new_block_meta = serialized.block_meta;
        let new_block_location = new_block_meta.location.0.clone();
        let new_block_raw_data = serialized.block_raw_data;
        let data_accessor = self.data_accessor.clone();
        write_data(new_block_raw_data, &data_accessor, &new_block_location).await?;
        if let Some(index_state) = serialized.bloom_index_state {
            write_data(index_state.data, &data_accessor, &index_state.location.0).await?;
        }

        // generate log
        let mutation = ReplacementLogEntry {
            index: BlockMetaIndex {
                segment_idx: segment_index,
                block_idx: block_index,
            },
            op: Replacement::Replaced(Arc::new(new_block_meta)),
        };

        Ok(Some(mutation))
    }

    fn overlapped(
        &self,
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        Self::check_overlap(&self.on_conflict_fields, column_stats, columns_min_max)
    }

    // if any item of `column_min_max` does NOT overlap with the corresponding item of `column_stats`
    // returns false, otherwise returns true.
    fn check_overlap(
        on_conflict_fields: &[OnConflictField],
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        for (idx, field) in on_conflict_fields.iter().enumerate() {
            let column_id = field.table_field.column_id();
            let (min, max) = &columns_min_max[idx];
            if !Self::check_overlapped_by_stats(column_stats.get(&column_id), min, max) {
                return false;
            }
        }
        true
    }

    fn check_overlapped_by_stats(
        column_stats: Option<&ColumnStatistics>,
        key_min: &Scalar,
        key_max: &Scalar,
    ) -> bool {
        if let Some(stats) = column_stats {
            std::cmp::min(key_max, &stats.max) >= std::cmp::max(key_min, &stats.min)
                || // coincide overlap
                (&stats.max == key_max && &stats.min == key_min)
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use common_expression::types::NumberDataType;
    use common_expression::types::NumberScalar;
    use common_expression::TableDataType;
    use common_expression::TableField;

    use super::*;

    #[test]
    fn test_check_overlap() -> Result<()> {
        // setup :
        //
        // - on conflict('xx_id', 'xx_type', 'xx_time');
        //
        // - range index of columns
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]

        // setup schema
        let field_type_id = TableDataType::Number(NumberDataType::UInt64);
        let field_type_string = TableDataType::String;
        let field_type_time = TableDataType::Number(NumberDataType::UInt32);

        let xx_id = TableField::new("xx_id", field_type_id);
        let xx_type = TableField::new("xx_type", field_type_string);
        let xx_time = TableField::new("xx_time", field_type_time);

        let schema = TableSchema::new(vec![xx_id, xx_type, xx_time]);

        let fields = schema.fields();

        // setup the ON CONFLICT fields
        let on_conflict_fields = fields
            .iter()
            .enumerate()
            .map(|(id, field)| OnConflictField {
                table_field: field.clone(),
                field_index: id,
            })
            .collect::<Vec<_>>();

        // set up range index of columns

        let range = |min: Scalar, max: Scalar| {
            ColumnStatistics {
                min,
                max,
                // the following values do not matter in this case
                null_count: 0,
                in_memory_size: 0,
                distinct_of_values: None,
            }
        };

        let column_range_indexes = HashMap::from_iter([
            // range of xx_id [1, 10]
            (
                0,
                range(
                    Scalar::Number(NumberScalar::UInt64(1)),
                    Scalar::Number(NumberScalar::UInt64(10)),
                ),
            ),
            // range of xx_type [a, z]
            (
                1,
                range(
                    Scalar::String("a".to_string().into_bytes()),
                    Scalar::String("z".to_string().into_bytes()),
                ),
            ),
            // range of xx_time [100, 200]
            (
                2,
                range(
                    Scalar::Number(NumberScalar::UInt32(100)),
                    Scalar::Number(NumberScalar::UInt32(200)),
                ),
            ),
        ]);

        // case 1:
        //
        // - min/max of input block
        //
        //  'xx_id' : [1, 9]
        //  'xx_type' : ["b", "y"]
        //  'xx_time' : [101, 200]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == true
        //   since value of all the ON CONFLICT columns of input block overlap with range index

        let input_column_min_max = [
            // for xx_id column, overlaps
            (
                Scalar::Number(NumberScalar::UInt64(1)),
                Scalar::Number(NumberScalar::UInt64(9)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string().into_bytes()),
                Scalar::String("y".to_string().into_bytes()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(101)),
                Scalar::Number(NumberScalar::UInt32(200)),
            ),
        ];

        let overlap = super::MergeIntoOperationAggregator::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(overlap);

        // case 2:
        //
        // - min/max of input block
        //
        //  'xx_id' : [11, 12]
        //  'xx_type' : ["b", "b"]
        //  'xx_time' : [100, 100]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == false
        //
        //   although columns 'xx_type' and 'xx_time' do overlap, but 'xx_id' does not overlap,
        //   so the result is NOT overlap

        let input_column_min_max = [
            // for xx_id column, NOT overlaps
            (
                Scalar::Number(NumberScalar::UInt64(11)),
                Scalar::Number(NumberScalar::UInt64(12)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string().into_bytes()),
                Scalar::String("b".to_string().into_bytes()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(100)),
                Scalar::Number(NumberScalar::UInt32(100)),
            ),
        ];

        let overlap = super::MergeIntoOperationAggregator::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(!overlap);

        Ok(())
    }
}
