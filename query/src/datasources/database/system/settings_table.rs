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

use common_catalog::IOContext;
use common_catalog::TableIOContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Extras;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::sessions::DatabendQueryContext;

pub struct SettingsTable {
    table_id: u64,
    schema: DataSchemaRef,
}

impl SettingsTable {
    pub fn create(table_id: u64) -> Self {
        SettingsTable {
            table_id,
            schema: DataSchemaRefExt::create(vec![
                DataField::new("name", DataType::String, false),
                DataField::new("value", DataType::String, false),
                DataField::new("default_value", DataType::String, false),
                DataField::new("description", DataType::String, false),
            ]),
        }
    }
}

#[async_trait::async_trait]
impl Table for SettingsTable {
    type PushDown = Extras;
    type ReadPlan = ReadDataSourcePlan;

    fn name(&self) -> &str {
        "settings"
    }

    fn engine(&self) -> &str {
        "SystemSettings"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn get_id(&self) -> u64 {
        self.table_id
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
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            table_id: self.table_id,
            table_version: None,
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::default(),
            description: "(Read from system.settings table)".to_string(),
            scan_plan: Default::default(), // scan_plan will be removed form ReadSourcePlan soon
            tbl_args: None,
            push_downs: None,
        })
    }

    async fn read(
        &self,
        io_ctx: Arc<TableIOContext>,
        _push_downs: &Option<Self::PushDown>,
    ) -> Result<SendableDataBlockStream> {
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");

        let settings = ctx.get_settings();

        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut default_values: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];
        for setting in settings.iter() {
            if let DataValue::Struct(vals) = setting {
                names.push(format!("{:?}", vals[0]));
                values.push(format!("{:?}", vals[1]));
                default_values.push(format!("{:?}", vals[2]));
                descs.push(format!("{:?}", vals[3]));
            }
        }

        let names: Vec<&[u8]> = names.iter().map(|x| x.as_bytes()).collect();
        let values: Vec<&[u8]> = values.iter().map(|x| x.as_bytes()).collect();
        let default_values: Vec<&[u8]> = default_values.iter().map(|x| x.as_bytes()).collect();
        let descs: Vec<&[u8]> = descs.iter().map(|x| x.as_bytes()).collect();
        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Series::new(names),
            Series::new(values),
            Series::new(default_values),
            Series::new(descs),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
