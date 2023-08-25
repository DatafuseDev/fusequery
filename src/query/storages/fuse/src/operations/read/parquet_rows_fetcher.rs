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
use std::sync::Arc;

use common_base::runtime::execute_futures_in_parallel;
use common_catalog::plan::block_idx_in_segment;
use common_catalog::plan::split_prefix;
use common_catalog::plan::split_row_id;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::Projection;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::TableSchemaRef;
use common_storage::ColumnNodes;
use itertools::Itertools;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::TableSnapshot;

use super::fuse_rows_fetcher::RowsFetcher;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::UncompressedBuffer;
use crate::FusePartInfo;
use crate::FuseTable;
use crate::MergeIOReadResult;

pub(super) struct ParquetRowsFetcher<const BLOCKING_IO: bool> {
    snapshot: Option<Arc<TableSnapshot>>,
    table: Arc<FuseTable>,
    segment_reader: CompactSegmentInfoReader,
    projection: Projection,
    schema: TableSchemaRef,

    settings: ReadSettings,
    reader: Arc<BlockReader>,
    uncompressed_buffer: Arc<UncompressedBuffer>,
    part_map: Arc<HashMap<u64, PartInfoPtr>>,

    // To control the parallelism of fetching blocks.
    max_threads: usize,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for ParquetRowsFetcher<BLOCKING_IO> {
    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        self.snapshot = self.table.read_table_snapshot().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn fetch(&mut self, row_ids: &[u64]) -> Result<DataBlock> {
        self.prepare_part_map(row_ids).await?;

        let num_rows = row_ids.len();
        let mut part_set = HashSet::new();
        let mut row_set = Vec::with_capacity(num_rows);
        for row_id in row_ids {
            let (prefix, idx) = split_row_id(*row_id);
            part_set.insert(prefix);
            row_set.push((prefix, idx));
        }

        // part_set.len() = parts_per_thread * max_threads + remain
        // task distribution:
        //   Part number of each task   |       Task number
        // ------------------------------------------------------
        //    parts_per_thread + 1      |         remain
        //      parts_per_thread        |   max_threads - remain
        let part_set = part_set.into_iter().sorted().collect::<Vec<_>>();
        let num_parts = part_set.len();
        let parts_per_thread = num_parts / self.max_threads;
        let remain = num_parts % self.max_threads as usize;
        let mut tasks = Vec::with_capacity(self.max_threads);
        // Fetch blocks in parallel.
        for i in 0..remain {
            let parts =
                part_set[i * (parts_per_thread + 1)..(i + 1) * (parts_per_thread + 1)].to_vec();
            tasks.push(Self::fetch_blocks(
                self.reader.clone(),
                parts,
                self.part_map.clone(),
                self.uncompressed_buffer.clone(),
                self.settings,
            ))
        }
        let offset = remain * (parts_per_thread + 1);
        for i in 0..(self.max_threads - remain) {
            let parts = part_set
                [offset + i * parts_per_thread..offset + (i + 1) * parts_per_thread]
                .to_vec();
            tasks.push(Self::fetch_blocks(
                self.reader.clone(),
                parts,
                self.part_map.clone(),
                self.uncompressed_buffer.clone(),
                self.settings,
            ))
        }

        let result = execute_futures_in_parallel(
            tasks,
            self.max_threads,
            self.max_threads * 2,
            "parqeut rows fetch".to_string(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
        // Merge blocks and idx_map.
        let mut blocks = Vec::with_capacity(num_parts);
        let mut idx_map = HashMap::with_capacity(num_parts);
        for (bs, m) in result {
            let offset = blocks.len();
            blocks.extend(bs);
            for (k, v) in m {
                idx_map.insert(k, v + offset);
            }
        }
        // Take result rows from blocks.
        let indices = row_set
            .iter()
            .map(|(prefix, row_idx)| {
                let block_idx = idx_map[prefix];
                (block_idx as u32, *row_idx as u32, 1_usize)
            })
            .collect::<Vec<_>>();

        Ok(DataBlock::take_blocks(&blocks, &indices, num_rows))
    }

    fn schema(&self) -> DataSchema {
        self.reader.data_schema()
    }
}

impl<const BLOCKING_IO: bool> ParquetRowsFetcher<BLOCKING_IO> {
    pub fn create(
        table: Arc<FuseTable>,
        projection: Projection,
        reader: Arc<BlockReader>,
        settings: ReadSettings,
        buffer_size: usize,
        max_threads: usize,
    ) -> Self {
        let uncompressed_buffer = UncompressedBuffer::new(buffer_size);
        let schema = table.schema();
        let segment_reader =
            MetaReaders::segment_info_reader(table.operator.clone(), schema.clone());
        Self {
            table,
            snapshot: None,
            segment_reader,
            projection,
            schema,
            reader,
            settings,
            uncompressed_buffer,
            part_map: Arc::new(HashMap::new()),
            max_threads,
        }
    }

    async fn prepare_part_map(&mut self, row_ids: &[u64]) -> Result<()> {
        let snapshot = self.snapshot.as_ref().unwrap();

        let arrow_schema = self.schema.to_arrow();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        let mut part_map = HashMap::new();
        for row_id in row_ids {
            let (prefix, _) = split_row_id(*row_id);

            if self.part_map.contains_key(&prefix) {
                continue;
            }

            let (segment, block) = split_prefix(prefix);
            let (location, ver) = snapshot.segments[segment as usize].clone();
            let compact_segment_info = self
                .segment_reader
                .read(&LoadParams {
                    ver,
                    location,
                    len_hint: None,
                    put_cache: true,
                })
                .await?;

            let blocks = compact_segment_info.block_metas()?;
            let block_idx = block_idx_in_segment(blocks.len(), block as usize);
            let block_meta = &blocks[block_idx];
            let part_info = FuseTable::projection_part(
                block_meta,
                &None,
                &column_nodes,
                None,
                &self.projection,
            );

            part_map.insert(prefix, part_info);
        }

        self.part_map = Arc::new(part_map);

        Ok(())
    }

    #[async_backtrace::framed]
    async fn fetch_blocks(
        reader: Arc<BlockReader>,
        part_set: Vec<u64>,
        part_map: Arc<HashMap<u64, PartInfoPtr>>,
        uncompressed_buffer: Arc<UncompressedBuffer>,
        settings: ReadSettings,
    ) -> Result<(Vec<DataBlock>, HashMap<u64, usize>)> {
        let mut chunks = Vec::with_capacity(part_set.len());
        if BLOCKING_IO {
            for prefix in part_set.into_iter() {
                let part = part_map[&prefix].clone();
                let chunk = reader.sync_read_columns_data_by_merge_io(&settings, part, &None)?;
                chunks.push((prefix, chunk));
            }
        } else {
            for prefix in part_set.into_iter() {
                let part = part_map[&prefix].clone();
                let part = FusePartInfo::from_part(&part)?;
                let chunk = reader
                    .read_columns_data_by_merge_io(
                        &settings,
                        &part.location,
                        &part.columns_meta,
                        &None,
                    )
                    .await?;
                chunks.push((prefix, chunk));
            }
        }
        let mut idx_map = HashMap::with_capacity(chunks.len());
        let fetched_blocks = chunks
            .into_iter()
            .enumerate()
            .map(|(idx, (part, chunk))| {
                idx_map.insert(part, idx);
                Self::build_block(
                    &reader,
                    &part_map[&part],
                    chunk,
                    uncompressed_buffer.clone(),
                )
            })
            .collect::<Result<Vec<_>>>()?;

        Ok((fetched_blocks, idx_map))
    }

    fn build_block(
        reader: &BlockReader,
        part: &PartInfoPtr,
        chunk: MergeIOReadResult,
        uncompressed_buffer: Arc<UncompressedBuffer>,
    ) -> Result<DataBlock> {
        let columns_chunks = chunk.columns_chunks()?;
        let part = FusePartInfo::from_part(part)?;
        reader.deserialize_parquet_chunks_with_buffer(
            &part.location,
            part.nums_rows,
            &part.compression,
            &part.columns_meta,
            columns_chunks,
            Some(uncompressed_buffer),
        )
    }
}
