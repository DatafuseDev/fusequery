// Copyright 2020 Datafuse Labs.
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
//

use common_exception::ErrorCode;
use common_flights::storage_api_impl::ReadPlanAction;
use common_flights::storage_api_impl::ReadPlanResult;
use common_flights::storage_api_impl::TruncateTableAction;
use common_flights::storage_api_impl::TruncateTableResult;
use log::debug;

use crate::executor::action_handler::RequestHandler;
use crate::executor::ActionHandler;

#[async_trait::async_trait]
impl RequestHandler<ReadPlanAction> for ActionHandler {
    async fn handle(&self, act: ReadPlanAction) -> common_exception::Result<ReadPlanResult> {
        let schema = &act.scan_plan.schema_name;
        let splits: Vec<&str> = schema.split('/').collect();
        // TODO error handling
        debug!("schema {}, splits {:?}", schema, splits);
        let db_name = splits[0];
        let tbl_name = splits[1];

        Ok(self.meta_node.get_data_parts(db_name, tbl_name).await)
    }
}

#[async_trait::async_trait]
impl RequestHandler<TruncateTableAction> for ActionHandler {
    async fn handle(
        &self,
        act: TruncateTableAction,
    ) -> common_exception::Result<TruncateTableResult> {
        let db_name = &act.db;
        let tbl_name = &act.table;

        let db = self.meta_node.get_database(db_name).await.ok_or_else(|| {
            ErrorCode::UnknownDatabase(format!("database not found {:}", db_name))
        })?;

        db.tables
            .get(tbl_name)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("table not found: {:}", tbl_name)))?;

        self.meta_node
            .remove_table_data_parts(db_name, tbl_name)
            .await;

        Ok(TruncateTableResult {})
    }
}
