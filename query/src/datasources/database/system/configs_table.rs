// Copyright 2021 Datafuse Labs.
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
use serde_json::Value;

use crate::catalogs::Table;
use crate::sessions::DatabendQueryContext;

pub struct ConfigsTable {
    table_id: u64,
    schema: DataSchemaRef,
}

impl ConfigsTable {
    pub fn create(table_id: u64) -> Self {
        ConfigsTable {
            table_id,
            schema: DataSchemaRefExt::create(vec![
                DataField::new("name", DataType::String, false),
                DataField::new("value", DataType::String, false),
                DataField::new("group", DataType::String, false),
                DataField::new("description", DataType::String, false),
            ]),
        }
    }

    fn extract_config(
        names: &mut Vec<String>,
        values: &mut Vec<String>,
        groups: &mut Vec<String>,
        descs: &mut Vec<String>,
        group: String,
        config_value: Value,
    ) {
        for (k, v) in config_value.as_object().unwrap().into_iter() {
            names.push(k.to_string());
            if let Value::String(s) = v {
                values.push(s.to_string());
            } else if let Value::Number(n) = v {
                values.push(n.to_string());
            }
            groups.push(group.clone());
            descs.push("".to_string());
        }
    }
}

#[async_trait::async_trait]
impl Table for ConfigsTable {
    type PushDown = Extras;
    type ReadPlan = ReadDataSourcePlan;

    fn name(&self) -> &str {
        "configs"
    }

    fn engine(&self) -> &str {
        "SystemConfigs"
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
            description: "(Read from system.configs table)".to_string(),
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

        let config = ctx.get_config();

        let mut names: Vec<String> = vec![];
        let mut values: Vec<String> = vec![];
        let mut groups: Vec<String> = vec![];
        let mut descs: Vec<String> = vec![];

        let query_config = config.query;
        let query_config_value = serde_json::to_value(query_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "query".to_string(),
            query_config_value,
        );

        let log_config = config.log;
        let log_config_value = serde_json::to_value(log_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "log".to_string(),
            log_config_value,
        );

        let meta_config = config.meta;
        let meta_config_value = serde_json::to_value(meta_config)?;
        ConfigsTable::extract_config(
            &mut names,
            &mut values,
            &mut groups,
            &mut descs,
            "meta".to_string(),
            meta_config_value,
        );

        let names: Vec<&str> = names.iter().map(|x| x.as_str()).collect();
        let values: Vec<&str> = values.iter().map(|x| x.as_str()).collect();
        let groups: Vec<&str> = groups.iter().map(|x| x.as_str()).collect();
        let descs: Vec<&str> = descs.iter().map(|x| x.as_str()).collect();
        let block = DataBlock::create_by_array(self.schema.clone(), vec![
            Series::new(names),
            Series::new(values),
            Series::new(groups),
            Series::new(descs),
        ]);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
