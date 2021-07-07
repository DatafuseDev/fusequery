// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

pub struct OneTable {
    schema: DataSchemaRef,
}

impl OneTable {
    pub fn create() -> Self {
        OneTable {
            schema: DataSchemaRefExt::create(vec![DataField::new("dummy", DataType::UInt8, false)]),
        }
    }
}

#[async_trait::async_trait]
impl Table for OneTable {
    fn name(&self) -> &str {
        "one"
    }

    fn engine(&self) -> &str {
        "SystemOne"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        true
    }

    fn read_plan(
        &self,
        _ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        Ok(ReadDataSourcePlan {
            db: "system".to_string(),
            table: self.name().to_string(),
            schema: self.schema.clone(),
            parts: vec![Part {
                name: "".to_string(),
                version: 0,
            }],
            statistics: Statistics::new_exact(1, std::mem::size_of::<u8>()),
            description: "(Read from system.one table)".to_string(),
            scan_plan: Arc::new(scan.clone()),
            remote: false,
        })
    }

    async fn read(
        &self,
        _ctx: FuseQueryContextRef,
        _read_source: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let block = DataBlock::create_by_array(self.schema.clone(), vec![Series::new(vec![1u8])]);
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
