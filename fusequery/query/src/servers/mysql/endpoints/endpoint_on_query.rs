// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::time::Instant;

use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::util::display::array_value_to_string;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use log::debug;
use log::error;
use msql_srv::*;

use crate::servers::mysql::endpoints::IMySQLEndpoint;

struct MySQLOnQueryEndpoint;

impl<'a, T: std::io::Write> IMySQLEndpoint<QueryResultWriter<'a, T>> for MySQLOnQueryEndpoint {
    type Input = Vec<DataBlock>;

    fn ok(blocks: Self::Input, dataset_writer: QueryResultWriter<'a, T>) -> std::io::Result<()> {
        // XXX: num_columns == 0 may is error?
        if blocks.is_empty() || (blocks[0].num_columns() == 0) {
            return dataset_writer.completed(0, 0);
        }

        fn convert_field_type(field: &Field) -> Result<ColumnType> {
            match field.data_type() {
                DataType::Int8 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Int16 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Int32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Int64 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt8 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt16 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::UInt64 => Ok(ColumnType::MYSQL_TYPE_LONG),
                DataType::Float32 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                DataType::Float64 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                DataType::Utf8 => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Boolean => Ok(ColumnType::MYSQL_TYPE_SHORT),
                DataType::Date32 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP),
                DataType::Date64 => Ok(ColumnType::MYSQL_TYPE_TIMESTAMP),
                _ => Err(ErrorCodes::UnImplement(format!(
                    "Unsupported column type:{:?}",
                    field.data_type()
                )))
            }
        }

        fn make_column_from_field(field: &Field) -> Result<Column> {
            convert_field_type(field).map(|column_type| Column {
                table: "".to_string(),
                column: field.name().to_string(),
                coltype: column_type,
                colflags: ColumnFlags::empty()
            })
        }

        fn convert_schema(schema: &DataSchemaRef) -> Result<Vec<Column>> {
            schema.fields().iter().map(make_column_from_field).collect()
        }

        let block = blocks[0].clone();
        match convert_schema(block.schema()) {
            Err(error) => MySQLOnQueryEndpoint::err(error, dataset_writer),
            Ok(columns) => {
                let columns_size = block.num_columns();
                dataset_writer.start(&columns).and_then(|mut row_writer| {
                    for block in &blocks {
                        let rows_size = block.column(0).len();
                        for row_index in 0..rows_size {
                            let mut row = Vec::with_capacity(columns_size);
                            for column_index in 0..columns_size {
                                let column = block.column(column_index).to_array().unwrap();
                                // We are already in convert_schema checks all supported to string columns
                                // So `array_value_to_string(column, r).unwrap()` is safe.
                                row.push(array_value_to_string(&column, row_index).unwrap());
                            }

                            row_writer.write_row(row)?;
                        }
                    }
                    row_writer.finish()
                })
            }
        }
    }

    fn err(error: ErrorCodes, writer: QueryResultWriter<'a, T>) -> std::io::Result<()> {
        error!("OnQuery Error: {:?}", error);
        writer.error(ErrorKind::ER_UNKNOWN_ERROR, format!("{}", error).as_bytes())
    }
}

type Input = Result<Vec<DataBlock>>;
type Output = std::io::Result<()>;

// TODO: Maybe can use generic to abstract all MySQLEndpoints done function
pub fn done<W: std::io::Write>(
    writer: QueryResultWriter<'_, W>
) -> impl FnOnce(Input) -> Output + '_ {
    move |res: Input| -> Output {
        match res {
            Err(error) => MySQLOnQueryEndpoint::err(error, writer),
            Ok(value) => {
                let start = Instant::now();
                let output = MySQLOnQueryEndpoint::ok(value, writer);
                debug!("MySQLHandler send to client cost:{:?}", start.elapsed());
                output
            }
        }
    }
}
