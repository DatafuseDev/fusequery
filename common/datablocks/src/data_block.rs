// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::convert::TryFrom;
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow;
use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::record_batch::RecordBatch;
use common_datavalues::columns::DataColumn;
use common_datavalues::series::IntoSeries;
use common_datavalues::series::Series;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::pretty_format_blocks;

#[derive(Clone)]
pub struct DataBlock {
    schema: DataSchemaRef,
    columns: Vec<DataColumn>,
}

impl DataBlock {
    pub fn create(schema: DataSchemaRef, columns: Vec<DataColumn>) -> Self {
        DataBlock { schema, columns }
    }

    pub fn create_by_array(schema: DataSchemaRef, arrays: Vec<Series>) -> Self {
        let columns = arrays
            .iter()
            .map(|array| DataColumn::Array(array.clone()))
            .collect();
        DataBlock { schema, columns }
    }

    pub fn empty() -> Self {
        DataBlock {
            schema: Arc::new(DataSchema::empty()),
            columns: vec![],
        }
    }

    pub fn empty_with_schema(schema: DataSchemaRef) -> Self {
        let mut columns = vec![];
        for f in schema.fields().iter() {
            let array = arrow::array::new_empty_array(f.data_type().to_arrow());
            let array: ArrayRef = Arc::from(array);
            columns.push(DataColumn::Array(array.into_series()))
        }
        DataBlock { schema, columns }
    }

    pub fn is_empty(&self) -> bool {
        self.num_columns() == 0 || self.num_rows() == 0
    }

    pub fn schema(&self) -> &DataSchemaRef {
        &self.schema
    }

    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Data Block physical memory size
    pub fn memory_size(&self) -> usize {
        self.columns.iter().map(|x| x.get_array_memory_size()).sum()
    }

    pub fn column(&self, index: usize) -> &DataColumn {
        &self.columns[index]
    }

    pub fn columns(&self) -> &[DataColumn] {
        &self.columns
    }

    pub fn try_column_by_name(&self, name: &str) -> Result<&DataColumn> {
        if name == "*" {
            Ok(&self.columns[0])
        } else {
            let idx = self.schema.index_of(name)?;
            Ok(&self.columns[idx])
        }
    }

    pub fn column_by_name(&self, name: &str) -> Option<&DataColumn> {
        if self.is_empty() {
            return None;
        }

        if name == "*" {
            return Some(&self.columns[0]);
        };

        if let Ok(idx) = self.schema.index_of(name) {
            Some(&self.columns[idx])
        } else {
            None
        }
    }

    pub fn try_array_by_name(&self, name: &str) -> Result<Series> {
        if name == "*" {
            self.columns[0].to_array()
        } else {
            let idx = self.schema.index_of(name)?;
            self.columns[idx].to_array()
        }
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let rows = self.num_rows();
        if offset == 0 && length >= rows {
            return self.clone();
        }
        let mut limited_columns = Vec::with_capacity(self.num_columns());
        for i in 0..self.num_columns() {
            limited_columns.push(self.column(i).slice(offset, length));
        }
        DataBlock::create(self.schema().clone(), limited_columns)
    }
}

impl TryFrom<DataBlock> for RecordBatch {
    type Error = ErrorCode;

    fn try_from(v: DataBlock) -> Result<RecordBatch> {
        let arrays = v
            .columns()
            .iter()
            .map(|c| c.to_array().map(|series| series.get_array_ref()))
            .collect::<Result<Vec<_>>>()?;

        Ok(RecordBatch::try_new(Arc::new(v.schema.to_arrow()), arrays)?)
    }
}

impl TryFrom<arrow::record_batch::RecordBatch> for DataBlock {
    type Error = ErrorCode;

    fn try_from(v: arrow::record_batch::RecordBatch) -> Result<DataBlock> {
        let schema = Arc::new(v.schema().as_ref().into());
        let series = v
            .columns()
            .iter()
            .map(|array| array.clone().into_series())
            .collect();
        Ok(DataBlock::create_by_array(schema, series))
    }
}

impl fmt::Debug for DataBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let formatted = pretty_format_blocks(&[self.clone()]).expect("Pretty format batches error");
        let lines: Vec<&str> = formatted.trim().lines().collect();
        write!(f, "\n{:#?}\n", lines)
    }
}
