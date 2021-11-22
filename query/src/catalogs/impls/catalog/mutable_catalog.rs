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

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::MetaApi;
use common_meta_embedded::MetaEmbedded;
use common_meta_types::CreateDatabaseReply;
use common_meta_types::CreateDatabaseReq;
use common_meta_types::DatabaseInfo;
use common_meta_types::DropDatabaseReq;
use common_meta_types::GetDatabaseReq;
use common_meta_types::MetaId;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_meta_types::UpsertTableOptionReply;
use common_meta_types::UpsertTableOptionReq;
use common_tracing::tracing;

use crate::catalogs::backends::MetaRemote;
use crate::catalogs::catalog::Catalog;
use crate::catalogs::database::Database;
use crate::catalogs::Table;
use crate::common::MetaClientProvider;
use crate::configs::Config;
use crate::datasources::context::DataSourceContext;
use crate::datasources::database::register_database_engines;
use crate::datasources::database_engine_registry::DatabaseEngineRegistry;
use crate::datasources::table::register_prelude_tbl_engines;
use crate::datasources::table_engine_registry::TableEngineRegistry;

/// Catalog based on MetaStore
/// - System Database NOT included
/// - Meta data of databases are saved in meta store
/// - Instances of `Database` are created by using database factories according to the engine
/// - Database engines are free to save table meta in metastore or not
#[derive(Clone)]
pub struct MutableCatalog {
    ctx: DataSourceContext,
}

impl MutableCatalog {
    /// The component hierarchy is layered as:
    /// ```text
    /// Remote:
    ///
    ///                                        RPC
    /// MetaRemote -------> Meta server      Meta server
    ///                     raft <---------> raft <----..
    ///                     MetaEmbedded     MetaEmbedded
    ///
    /// Embedded:
    ///
    /// MetaEmbedded
    /// ```
    pub async fn try_create_with_config(conf: Config) -> Result<Self> {
        let local_mode = conf.meta.meta_address.is_empty();

        let meta: Arc<dyn MetaApi> = if local_mode {
            tracing::info!("use embedded meta");
            // TODO(xp): This can only be used for test: data will be removed when program quit.

            let meta_embedded = MetaEmbedded::new_temp().await?;
            Arc::new(meta_embedded)
        } else {
            tracing::info!("use remote meta");

            let meta_client_provider =
                Arc::new(MetaClientProvider::new(conf.meta.to_flight_client_config()));
            let meta_remote = MetaRemote::create(meta_client_provider);
            Arc::new(meta_remote)
        };

        // Register database and table engine.
        let database_engine_registry = Arc::new(DatabaseEngineRegistry::default());
        register_database_engines(&database_engine_registry)?;
        let table_engine_registry = Arc::new(TableEngineRegistry::default());
        register_prelude_tbl_engines(&table_engine_registry)?;

        // Create default database.
        let req = CreateDatabaseReq {
            if_not_exists: true,
            db: "default".to_string(),
            engine: "".to_string(),
            options: Default::default(),
        };
        meta.create_database(req).await?;

        let ctx = DataSourceContext {
            meta,
            table_engine_registry,
            database_engine_registry,
            in_memory_data: Arc::new(Default::default()),
        };
        Ok(MutableCatalog { ctx })
    }

    fn build_db_instance(&self, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database>> {
        // TODO(bohu): Add the database engine match, now we set only one fuse database here, like:
        // match db_info.engine {
        //    "default" ->  create fuse database
        //    "github" ->  create github database
        // }
        let engine = "DEFAULT";
        let factory = self
            .ctx
            .database_engine_registry
            .get_database_factory(engine)
            .ok_or_else(|| {
                ErrorCode::UnknownDatabaseEngine(format!("unknown database engine {}", engine))
            })?;

        let db: Arc<dyn Database> = factory.try_create(&db_info.db, self.ctx.clone())?.into();
        Ok(db)
    }
}

#[async_trait::async_trait]
impl Catalog for MutableCatalog {
    async fn get_database(&self, db_name: &str) -> Result<Arc<dyn Database>> {
        let db_info = self
            .ctx
            .meta
            .get_database(GetDatabaseReq::new(db_name))
            .await?;
        self.build_db_instance(&db_info)
    }

    async fn list_databases(&self) -> Result<Vec<Arc<dyn Database>>> {
        let dbs = self.ctx.meta.list_databases().await?;

        dbs.iter().try_fold(vec![], |mut acc, item| {
            let db = self.build_db_instance(item)?;
            acc.push(db);
            Ok(acc)
        })
    }

    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        self.ctx.meta.create_database(req).await
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<()> {
        self.ctx.meta.drop_database(req).await?;
        Ok(())
    }

    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let engine = table_info.engine();
        let factory = self
            .ctx
            .table_engine_registry
            .get_table_factory(engine)
            .ok_or_else(|| {
                ErrorCode::UnknownTableEngine(format!("unknown table engine {}", engine))
            })?;

        let tbl: Arc<dyn Table> = factory
            .try_create(table_info.clone(), self.ctx.clone())?
            .into();

        Ok(tbl)
    }

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        self.ctx.meta.upsert_table_option(req).await
    }

    async fn get_table_meta_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        self.ctx.meta.get_table_by_id(table_id).await
    }
}
