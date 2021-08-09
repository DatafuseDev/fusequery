// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::mpsc::channel;
use std::sync::Arc;

use common_datavalues::DataSchemaRef;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::InsertIntoPlan;
use common_planners::Part;
use common_planners::ReadDataSourcePlan;
use common_planners::ScanPlan;
use common_planners::Statistics;
use common_planners::TableOptions;
use common_planners::TruncateTablePlan;
use common_store_api::ReadPlanResult;
use common_streams::SendableDataBlockStream;

use crate::datasources::remote::store_client_provider::StoreApis;
use crate::datasources::remote::StoreApisProvider;
use crate::datasources::Table;
use crate::sessions::FuseQueryContextRef;

#[allow(dead_code)]
pub struct RemoteTable<T> {
    pub(crate) db: String,
    pub(crate) name: String,
    pub(crate) schema: DataSchemaRef,
    pub(crate) store_api_provider: StoreApisProvider<T>,
}

#[async_trait::async_trait]
impl<T> Table for RemoteTable<T>
where T: 'static + StoreApis + Clone
{
    fn name(&self) -> &str {
        &self.name
    }

    fn engine(&self) -> &str {
        "remote"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }

    fn is_local(&self) -> bool {
        false
    }

    fn read_plan(
        &self,
        ctx: FuseQueryContextRef,
        scan: &ScanPlan,
        _partitions: usize,
    ) -> Result<ReadDataSourcePlan> {
        // Change this method to async at current stage might be harsh
        let (tx, rx) = channel();
        let cli_provider = self.store_api_provider.clone();
        let db_name = self.db.clone();
        let tbl_name = self.name.clone();
        {
            let scan = scan.clone();
            ctx.execute_task(async move {
                match cli_provider.try_get_store_apis().await {
                    Ok(mut client) => {
                        let parts_info = client
                            .read_plan(db_name, tbl_name, &scan)
                            .await
                            .map_err(ErrorCode::from);
                        let _ = tx.send(parts_info);
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e));
                    }
                }
            })?;
        }

        rx.recv()
            .map_err(ErrorCode::from_std_error)?
            .map(|v| self.partitions_to_plan(v, scan.clone()))
    }

    async fn read(
        &self,
        ctx: FuseQueryContextRef,
        source_plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        self.do_read(ctx, source_plan).await
    }

    async fn append_data(&self, _ctx: FuseQueryContextRef, plan: InsertIntoPlan) -> Result<()> {
        let opt_stream = {
            let mut inner = plan.input_stream.lock();
            (*inner).take()
        };

        {
            let block_stream =
                opt_stream.ok_or_else(|| ErrorCode::EmptyData("input stream consumed"))?;

            let mut client = self.store_api_provider.try_get_store_apis().await?;

            client
                .append_data(
                    plan.db_name.clone(),
                    plan.tbl_name.clone(),
                    (&plan).schema().clone(),
                    block_stream,
                )
                .await?;
        }

        Ok(())
    }

    async fn truncate(&self, _ctx: FuseQueryContextRef, plan: TruncateTablePlan) -> Result<()> {
        let mut client = self.store_api_provider.try_get_store_apis().await?;
        client.truncate(plan.db.clone(), plan.table.clone()).await?;
        Ok(())
    }
}

impl<T> RemoteTable<T>
where T: 'static + StoreApis + Clone
{
    pub fn create(
        db: impl Into<String>,
        name: impl Into<String>,
        schema: DataSchemaRef,
        store_client_provider: StoreApisProvider<T>,
        _options: TableOptions,
    ) -> Box<dyn Table> {
        let table = Self {
            db: db.into(),
            name: name.into(),
            schema,
            store_api_provider: store_client_provider,
        };
        Box::new(table)
    }

    fn partitions_to_plan(&self, res: ReadPlanResult, scan_plan: ScanPlan) -> ReadDataSourcePlan {
        let mut partitions = vec![];
        let mut statistics = Statistics {
            read_rows: 0,
            read_bytes: 0,
            is_exact: false,
        };

        if let Some(parts) = res {
            for part in parts {
                partitions.push(Part {
                    name: part.part.name,
                    version: 0,
                });
                statistics.read_rows += part.stats.read_rows;
                statistics.read_bytes += part.stats.read_bytes;
                statistics.is_exact &= part.stats.is_exact;
            }
        }

        ReadDataSourcePlan {
            db: self.db.clone(),
            table: self.name.clone(),
            table_id: scan_plan.table_id,
            table_version: scan_plan.table_version,
            schema: self.schema.clone(),
            parts: partitions,
            statistics,
            description: "".to_string(),
            scan_plan: Arc::new(scan_plan),
            remote: true,
        }
    }
}
