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

use std::any::Any;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::Expression;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::storages::fuse::table_functions::table_arg_util::parse_func_history_args;
use crate::storages::fuse::table_functions::table_arg_util::string_literal;
use crate::storages::fuse::FuseHistory;
use crate::storages::fuse::FuseTable;
use crate::storages::Table;
use crate::table_functions::TableArgs;
use crate::table_functions::TableFunction;

pub const FUSE_FUNC_HIST: &str = "fuse_history";

pub struct FuseHistoryTable {
    table_info: TableInfo,
    arg_database_name: String,
    arg_table_name: String,
}

impl FuseHistoryTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let (arg_database_name, arg_table_name) = parse_func_history_args(&table_args)?;

        let engine = FUSE_FUNC_HIST.to_owned();

        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: FuseHistory::schema(),
                engine,
                ..Default::default()
            },
        };

        Ok(Arc::new(FuseHistoryTable {
            table_info,
            arg_database_name,
            arg_table_name,
        }))
    }
}

#[async_trait::async_trait]
impl Table for FuseHistoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    fn table_args(&self) -> Option<Vec<Expression>> {
        Some(vec![
            string_literal(self.arg_database_name.as_str()),
            string_literal(self.arg_table_name.as_str()),
        ])
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog()
            .get_table(
                tenant_id.as_str(),
                self.arg_database_name.as_str(),
                self.arg_table_name.as_str(),
            )
            .await?;

        let tbl = tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::BadArguments(format!(
                "expecting fuse table, but got table of engine type: {}",
                tbl.get_table_info().meta.engine
            ))
        })?;

        let blocks = vec![FuseHistory::new(ctx.clone(), tbl).get_history().await?];
        Ok(Box::pin(DataBlockStream::create(
            FuseHistory::schema(),
            None,
            blocks,
        )))
    }
}

impl TableFunction for FuseHistoryTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
