// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.
mod csv_table;
mod local_factory;
mod null_table;

mod csv_table_test;
mod null_table_test;
mod parquet_table;
mod parquet_table_test;

pub use csv_table::CSVTable;
pub use local_factory::LocalFactory;
pub use null_table::NullTable;
pub use parquet_table::ParquetTable;
