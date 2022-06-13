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

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_datavalues::TypeSerializer;
use common_exception::Result;
use common_io::prelude::FormatSettings;

use crate::formats::output_format::OutputFormat;

#[derive(Default)]
pub struct JsonEachRowOutputFormat {}

impl JsonEachRowOutputFormat {
    pub fn create(_schema: DataSchemaRef) -> Self {
        Self {}
    }
}

impl OutputFormat for JsonEachRowOutputFormat {
    fn serialize_block(&mut self, block: &DataBlock, format: &FormatSettings) -> Result<Vec<u8>> {
        let rows_size = block.column(0).len();

        let mut buf = Vec::with_capacity(block.memory_size());
        let serializers = block.get_serializers()?;
        let field_names: Vec<_> = block
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().as_bytes())
            .collect();

        for row_index in 0..rows_size {
            for (col_index, serializer) in serializers.iter().enumerate() {
                if col_index != 0 {
                    buf.push(b',');
                } else {
                    buf.push(b'{')
                }

                buf.push(b'"');
                buf.extend_from_slice(field_names[col_index]);
                buf.push(b'"');

                buf.push(b':');
                serializer.write_field_quoted(row_index, &mut buf, format, b'\"');
            }
            buf.extend_from_slice("}\n".as_bytes());
        }
        Ok(buf)
    }

    fn finalize(&mut self) -> Result<Vec<u8>> {
        Ok(vec![])
    }
}
