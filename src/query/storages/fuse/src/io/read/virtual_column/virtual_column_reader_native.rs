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

use common_arrow::native::read as nread;
use storages_common_table_meta::meta::ColumnMeta;

use super::VirtualColumnReader;
use crate::io::BlockReader;
use crate::io::NativeSourceData;

impl VirtualColumnReader {
    pub fn sync_read_native_data(&self, loc: &str) -> Option<NativeSourceData> {
        match self.reader.operator.blocking().reader(loc) {
            Ok(mut reader) => {
                let metadata = nread::reader::read_meta(&mut reader).ok()?;
                let schema = nread::reader::infer_schema(&mut reader).ok()?;

                let num_rows: u64 = metadata[0].pages.iter().map(|p| p.num_values).sum();
                debug_assert!(
                    metadata
                        .iter()
                        .all(|c| c.pages.iter().map(|p| p.num_values).sum::<u64>() == num_rows)
                );

                let mut results = NativeSourceData::new();
                for (index, virtual_column) in self.virtual_column_infos.iter().enumerate() {
                    for (i, f) in schema.fields.iter().enumerate() {
                        if f.name == virtual_column.name {
                            let metas = vec![ColumnMeta::Native(metadata[i].clone())];
                            let readers = BlockReader::sync_read_native_column(
                                self.dal.clone(),
                                loc,
                                metas,
                                None,
                            )
                            .ok()?;

                            let virtual_index = self.source_schema.num_fields() + index;
                            results.insert(virtual_index, readers);
                            break;
                        }
                    }
                }
                if !results.is_empty() {
                    Some(results)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    pub async fn read_native_data(&self, loc: &str) -> Option<NativeSourceData> {
        match self.reader.operator.reader(loc).await {
            Ok(mut reader) => {
                let metadata = nread::reader::read_meta_async(&mut reader, None)
                    .await
                    .ok()?;
                let schema = nread::reader::infer_schema_async(&mut reader).await.ok()?;

                let num_rows: u64 = metadata[0].pages.iter().map(|p| p.num_values).sum();
                debug_assert!(
                    metadata
                        .iter()
                        .all(|c| c.pages.iter().map(|p| p.num_values).sum::<u64>() == num_rows)
                );

                let mut results = NativeSourceData::new();
                for (index, virtual_column) in self.virtual_column_infos.iter().enumerate() {
                    for (i, f) in schema.fields.iter().enumerate() {
                        if f.name == virtual_column.name {
                            let metas = vec![ColumnMeta::Native(metadata[i].clone())];
                            let (_, readers) = BlockReader::read_native_columns_data(
                                self.dal.clone(),
                                loc,
                                i,
                                metas,
                                None,
                            )
                            .await
                            .ok()?;

                            let virtual_index = self.source_schema.num_fields() + index;
                            results.insert(virtual_index, readers);
                            break;
                        }
                    }
                }
                if !results.is_empty() {
                    Some(results)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}
