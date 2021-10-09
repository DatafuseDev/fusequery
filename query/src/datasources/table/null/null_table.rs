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

use std::any::Any;
use std::sync::Arc;

use common_catalog::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_flight::meta_flight_reply::TableInfo;
use common_planners::Extras;
use common_planners::InsertIntoPlan;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing::info;
use futures::stream::StreamExt;

use crate::catalogs::Table;

pub struct NullTable {
    tbl_info: TableInfo,
}

impl NullTable {
    pub fn try_create(
        tbl_info: TableInfo,
    ) -> Result<Box<dyn Table<PushDown = Extras, ReadPlan = ReadDataSourcePlan>>> {
        Ok(Box::new(Self { tbl_info }))
    }
}

#[async_trait::async_trait]
impl Table for NullTable {
    type PushDown = Extras;
    type ReadPlan = ReadDataSourcePlan;

    fn name(&self) -> &str {
        &self.tbl_info.name
    }

    fn engine(&self) -> &str {
        &self.tbl_info.engine
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.tbl_info.schema.clone())
    }

    fn get_id(&self) -> u64 {
        self.tbl_info.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _push_downs: Option<Self::PushDown>,
        _partition_num_hint: Option<usize>,
    ) -> Result<Self::ReadPlan> {
        let tbl_info = &self.tbl_info;
        let db = &tbl_info.db;
        Ok(ReadDataSourcePlan {
            db: db.to_string(),
            table: self.name().to_string(),
            table_id: tbl_info.table_id,
            table_version: None,
            schema: tbl_info.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::new_exact(0, 0),
            description: format!("(Read from Null Engine table  {}.{})", db, self.name()),
            scan_plan: Default::default(),
            tbl_args: None,
            push_downs: None,
        })
    }

    async fn read(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _push_downs: &Option<Self::PushDown>,
    ) -> Result<SendableDataBlockStream> {
        let block = DataBlock::empty_with_schema(self.tbl_info.schema.clone());

        Ok(Box::pin(DataBlockStream::create(
            self.tbl_info.schema.clone(),
            None,
            vec![block],
        )))
    }

    async fn append_data(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _insert_plan: InsertIntoPlan,
    ) -> Result<()> {
        let mut s = {
            let mut inner = _insert_plan.input_stream.lock();
            (*inner).take()
        }
        .ok_or_else(|| ErrorCode::EmptyData("input stream consumed"))?;

        while let Some(block) = s.next().await {
            info!("Ignore one block rows: {}", block.num_rows())
        }
        Ok(())
    }

    async fn truncate(
        &self,
        _io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Ok(())
    }
}
