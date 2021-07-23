// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use common::Common;
pub use database::Database;
pub use remote::RemoteFactory;
pub use table::Table;
pub use table::TablePtr;
pub use table_function::TableFunction;

pub use crate::catalog::database_catalog::DatabaseCatalog;
pub use crate::catalog::datasource_meta::TableFunctionMeta;
pub use crate::catalog::datasource_meta::TableMeta;

#[cfg(test)]
mod common_test;
#[cfg(test)]
mod tests;

mod common;
mod database;
pub(crate) mod local;
pub(crate) mod remote;
pub(crate) mod system;
mod table;
mod table_function;
