// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_io::prelude::FormatSettings;
use opensrv_clickhouse::types::column::NullableColumnData;
use serde_json::Value;
use streaming_iterator::StreamingIterator;

use crate::prelude::DataValue;
use crate::serializations::formats::iterators::NullInfo;
use crate::Column;
use crate::ColumnRef;
use crate::NullableColumn;
use crate::Series;
use crate::TypeSerializer;
use crate::TypeSerializerImpl;

#[derive(Debug, Clone)]
pub struct NullableSerializer {
    pub inner: Box<TypeSerializerImpl>,
}

impl TypeSerializer for NullableSerializer {
    fn serialize_value(&self, value: &DataValue, format: &FormatSettings) -> Result<String> {
        if value.is_null() {
            Ok("NULL".to_owned())
        } else {
            self.inner.serialize_value(value, format)
        }
    }

    fn serialize_column(&self, column: &ColumnRef, format: &FormatSettings) -> Result<Vec<String>> {
        let column: &NullableColumn = Series::check_get(column)?;
        let rows = column.len();
        let mut res = self.inner.serialize_column(column.inner(), format)?;

        (0..rows).for_each(|row| {
            if column.null_at(row) {
                res[row] = "NULL".to_owned();
            }
        });
        Ok(res)
    }

    fn serialize_column_quoted(
        &self,
        column: &ColumnRef,
        format: &FormatSettings,
    ) -> Result<Vec<String>> {
        let column: &NullableColumn = Series::check_get(column)?;
        let rows = column.len();
        let mut res = self.inner.serialize_column_quoted(column.inner(), format)?;

        (0..rows).for_each(|row| {
            if column.null_at(row) {
                res[row] = "NULL".to_owned();
            }
        });
        Ok(res)
    }

    fn serialize_json(&self, column: &ColumnRef, format: &FormatSettings) -> Result<Vec<Value>> {
        let column: &NullableColumn = Series::check_get(column)?;
        let rows = column.len();
        let mut res = self.inner.serialize_json(column.inner(), format)?;

        (0..rows).for_each(|row| {
            if column.null_at(row) {
                res[row] = Value::Null;
            }
        });
        Ok(res)
    }

    fn serialize_clickhouse_format(
        &self,
        column: &ColumnRef,
        format: &FormatSettings,
    ) -> Result<opensrv_clickhouse::types::column::ArcColumnData> {
        let column: &NullableColumn = Series::check_get(column)?;
        let inner = self
            .inner
            .serialize_clickhouse_format(column.inner(), format)?;
        let nulls = column.ensure_validity().iter().map(|v| !v as u8).collect();
        let data = NullableColumnData { nulls, inner };

        Ok(Arc::new(data))
    }

    fn serialize_csv<'a>(
        &self,
        column: &'a ColumnRef,
        format: &FormatSettings,
    ) -> Result<Box<dyn StreamingIterator<Item = [u8]> + 'a>> {
        let column2: &NullableColumn = Series::check_get(&column)?;
        let null_helper = NullInfo::new(|row| column.null_at(row), vec![b'\0']);
        let it = self
            .inner
            .serialize_csv_inner(column2.inner(), format, null_helper);
        it
    }

    fn serialize_csv_inner<'a, F2>(
        &self,
        _column: &'a ColumnRef,
        _format: &FormatSettings,
        _nullable: NullInfo<F2>,
    ) -> Result<Box<dyn StreamingIterator<Item = [u8]> + 'a>>
    where
        F2: Fn(usize) -> bool + 'a,
    {
        unreachable!()
    }
}
