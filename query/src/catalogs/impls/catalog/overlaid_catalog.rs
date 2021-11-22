//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::CreateTableReq;
use common_meta_types::DropDatabaseReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;

use crate::catalogs::Catalog;
use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::catalogs::TableFunction;
use crate::datasources::table_func_engine::TableArgs;
use crate::datasources::table_func_engine::TableFuncEngine;
use crate::datasources::table_func_engine_registry::TableFuncEngineRegistry;

/// Combine two catalogs together
/// - read/search like operations are always performed at
///   upper layer first, and bottom layer later(if necessary)  
/// - metadata are written to the bottom layer
#[derive(Clone)]
pub struct OverlaidCatalog {
    /// the upper layer, read only
    read_only: Arc<dyn Catalog + Send + Sync>,
    /// bottom layer, writing goes here
    bottom: Arc<dyn Catalog + Send + Sync>,
    /// table function engine factories
    func_engine_registry: TableFuncEngineRegistry,
}

impl OverlaidCatalog {
    pub fn create(
        upper_read_only: Arc<dyn Catalog + Send + Sync>,
        bottom: Arc<dyn Catalog + Send + Sync>,
        func_engine_registry: HashMap<String, (u64, Arc<dyn TableFuncEngine>)>,
    ) -> Self {
        Self {
            read_only: upper_read_only,
            bottom,
            func_engine_registry,
        }
    }
}

#[async_trait::async_trait]
impl Catalog for OverlaidCatalog {
    async fn list_databases(&self) -> common_exception::Result<Vec<Arc<dyn Database>>> {
        let mut dbs = self.read_only.list_databases().await?;
        let mut other = self.bottom.list_databases().await?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    async fn get_database(&self, db_name: &str) -> common_exception::Result<Arc<dyn Database>> {
        let r = self.read_only.get_database(db_name).await;
        match r {
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_database(db_name).await
                } else {
                    Err(e)
                }
            }
            Ok(db) => Ok(db),
        }
    }

    async fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        let res = self.read_only.get_table(db_name, table_name).await;
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.get_table(db_name, table_name).await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn list_tables(&self, db_name: &str) -> common_exception::Result<Vec<Arc<dyn Table>>> {
        let r = self.read_only.list_tables(db_name).await;
        match r {
            Ok(x) => Ok(x),
            Err(e) => {
                if e.code() == ErrorCode::UnknownDatabase("").code() {
                    self.bottom.list_tables(db_name).await
                } else {
                    Err(e)
                }
            }
        }
    }

    async fn get_table_meta_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        let res = self.read_only.get_table_meta_by_id(table_id).await;

        if let Ok(x) = res {
            Ok(x)
        } else {
            self.bottom.get_table_meta_by_id(table_id).await
        }
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<(), ErrorCode> {
        self.bottom.create_table(req).await
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply, ErrorCode> {
        let r = self.read_only.drop_table(req.clone()).await;
        match r {
            Err(e) => {
                if e.code() == ErrorCode::UnknownTable("").code() {
                    self.bottom.drop_table(req).await
                } else {
                    Err(e)
                }
            }
            Ok(x) => Ok(x),
        }
    }

    fn build_table(&self, table_info: &TableInfo) -> common_exception::Result<Arc<dyn Table>> {
        let res = self.read_only.build_table(table_info);
        match res {
            Ok(t) => Ok(t),
            Err(e) => {
                if e.code() == ErrorCode::UnknownTable("").code() {
                    self.bottom.build_table(table_info)
                } else {
                    Err(e)
                }
            }
        }
    }

    fn get_table_function(
        &self,
        func_name: &str,
        tbl_args: TableArgs,
    ) -> common_exception::Result<Arc<dyn TableFunction>> {
        let (id, factory) = self.func_engine_registry.get(func_name).ok_or_else(|| {
            ErrorCode::UnknownTable(format!("unknown table function {}", func_name))
        })?;

        // table function belongs to no/every database
        let func = factory.try_create("", func_name, *id, tbl_args)?;
        Ok(func)
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> common_exception::Result<UpsertTableOptionReply> {
        // upsert table option in BOTTOM layer only
        self.bottom.upsert_table_option(req).await
    }

    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> common_exception::Result<CreateDatabaseReply> {
        if self.read_only.exists_database(&req.db).await? {
            return Err(ErrorCode::DatabaseAlreadyExists(format!(
                "{} database exists",
                req.db
            )));
        }
        // create db in BOTTOM layer only
        self.bottom.create_database(req).await
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<(), ErrorCode> {
        // drop db in BOTTOM layer only
        if self.read_only.exists_database(&req.db).await? {
            return Err(ErrorCode::UnexpectedError(format!(
                "user can not drop {} database",
                req.db
            )));
        }
        self.bottom.drop_database(req).await
    }
}
