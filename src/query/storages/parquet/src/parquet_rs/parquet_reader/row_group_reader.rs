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

use std::sync::Arc;

use bytes::Bytes;
use common_arrow::arrow::bitmap::Bitmap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::arrow::arrow_reader::RowSelection;
use parquet::arrow::arrow_reader::RowSelector;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::ProjectionMask;
use parquet::column::page::PageIterator;
use parquet::column::page::PageReader;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::reader::SerializedPageReader;

pub struct InMemoryRowGroup {
    pub metadata: RowGroupMetaData,
    pub column_chunks: Vec<Arc<Bytes>>,
}

impl InMemoryRowGroup {
    // pub fn new(metadata: RowGroupMetaData, column_chunks: Vec<Vec<u8>>) -> Self {
    //     InMemoryRowGroup {
    //         metadata,
    //         column_chunks: column_chunks
    //             .iter()
    //             .map(|x| Arc::new(Bytes::from(x.clone())))
    //             .collect(),
    //     }
    // }

    pub fn get_record_batch_reader(
        &self,
        batch_size: usize,
        selection: Option<RowSelection>,
    ) -> parquet::errors::Result<ParquetRecordBatchReader> {
        let levels = parquet_to_arrow_field_levels(
            &self.metadata.schema_descr_ptr(),
            ProjectionMask::all(),
            None,
        )?;

        ParquetRecordBatchReader::try_new_with_row_groups(&levels, self, batch_size, selection)
    }
}

impl RowGroups for InMemoryRowGroup {
    fn num_rows(&self) -> usize {
        self.metadata.num_rows() as usize
    }

    fn column_chunks(&self, i: usize) -> parquet::errors::Result<Box<dyn PageIterator>> {
        let page_reader: Box<dyn PageReader> = Box::new(SerializedPageReader::new(
            self.column_chunks[i].clone(),
            self.metadata.column(i),
            self.num_rows(),
            None,
        )?);

        Ok(Box::new(ColumnChunkIterator {
            reader: Some(Ok(page_reader)),
        }))
    }
}

/// Implements [`PageIterator`] for a single column chunk, yielding a single [`PageReader`]
struct ColumnChunkIterator {
    reader: Option<parquet::errors::Result<Box<dyn PageReader>>>,
}

impl Iterator for ColumnChunkIterator {
    type Item = parquet::errors::Result<Box<dyn PageReader>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.reader.take()
    }
}

impl PageIterator for ColumnChunkIterator {}

pub fn bitmap_to_selection(bitmap: Bitmap) -> RowSelection {
    let mut selectors = Vec::<RowSelector>::new();
    if let Some(v) = bitmap.get(0) {
        let curr = v;
        let len = bitmap.len();
        let mut start = 0;
        for (i, v) in bitmap.iter().skip(1).enumerate() {
            if v != curr {
                selectors.push(RowSelector {
                    row_count: i - start,
                    skip: !curr,
                });
                start = i;
            }
        }

        if start != len - 1 {
            selectors.push(RowSelector {
                row_count: len - start,
                skip: !curr,
            });
        }
    };

    RowSelection::from(selectors)
}
