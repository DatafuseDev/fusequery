// Copyright 2023 Datafuse Labs.
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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use arrow_flight::FlightData;
use arrow_flight::SchemaAsIpc;
use arrow_ipc::writer;
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::Schema as ArrowSchema;
use common_base::base::tokio;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_sql::plans::Plan;
use common_sql::PlanExtras;
use common_sql::Planner;
use common_storages_fuse::TableContext;
use futures::Stream;
use futures::StreamExt;
use serde::Deserialize;
use serde::Serialize;
use tonic::Status;

use super::status;
use super::DoGetStream;
use super::FlightSqlServiceImpl;
use crate::interpreters::InterpreterFactory;
use crate::sessions::Session;

/// A app_metakey which indicates the data is a progress type
static H_PROGRESS: u8 = 0x01;

impl FlightSqlServiceImpl {
    pub(crate) fn schema_to_flight_data(data_schema: DataSchema) -> FlightData {
        let arrow_schema = ArrowSchema::from(&data_schema);
        let options = IpcWriteOptions::default();
        SchemaAsIpc::new(&arrow_schema, &options).into()
    }

    pub fn block_to_flight_data(block: DataBlock, data_schema: &DataSchema) -> Result<FlightData> {
        let batch = block
            .to_record_batch(data_schema)
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;
        let options = IpcWriteOptions::default();
        let data_gen = writer::IpcDataGenerator::default();
        let mut dictionary_tracker = writer::DictionaryTracker::new(false);

        let (_encoded_dictionaries, encoded_batch) = data_gen
            .encoded_batch(&batch, &mut dictionary_tracker, &options)
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;

        Ok(encoded_batch.into())
    }

    #[async_backtrace::framed]
    pub(super) async fn plan_sql(
        &self,
        session: &Arc<Session>,
        query: &str,
    ) -> Result<(Plan, PlanExtras)> {
        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;

        let mut planner = Planner::new(context.clone());
        planner.plan_sql(query).await
    }

    #[async_backtrace::framed]
    pub(super) async fn execute_update(
        &self,
        session: Arc<Session>,
        plan: &Plan,
        plan_extras: &PlanExtras,
    ) -> Result<i64> {
        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;

        context.attach_query_str(plan.to_string(), plan_extras.stament.to_mask_sql());
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;

        let mut blocks = interpreter.execute(context.clone()).await?;
        while let Some(block) = blocks.next().await {
            block?;
        }

        let affected_rows = context.get_write_progress_value().rows;
        Ok(affected_rows as i64)
    }

    pub(super) async fn execute_query(
        &self,
        session: Arc<Session>,
        plan: &Plan,
        plan_extras: &PlanExtras,
    ) -> Result<DoGetStream> {
        let is_native_client = session.get_status().read().is_native_client;

        let context = session
            .create_query_context()
            .await
            .map_err(|e| status!("Could not create_query_context", e))?;

        context.attach_query_str(plan.to_string(), plan_extras.stament.to_mask_sql());
        let interpreter = InterpreterFactory::get(context.clone(), plan).await?;

        let data_schema = interpreter.schema();
        let data_stream = interpreter.execute(context.clone()).await?;

        let is_finished = Arc::new(AtomicBool::new(false));
        let is_finished_clone = is_finished.clone();
        let (sender, receiver) = tokio::sync::mpsc::channel(2);
        let _ = sender
            .send(Ok(Self::schema_to_flight_data((*data_schema).clone())))
            .await;

        let s1 = sender.clone();
        tokio::spawn(async move {
            let mut data_stream = data_stream;

            while let Some(block) = data_stream.next().await {
                match block {
                    Ok(block) => {
                        let res =
                            match FlightSqlServiceImpl::block_to_flight_data(block, &data_schema) {
                                Ok(flight_data) => Ok(flight_data),
                                Err(err) => Err(status!("Could not convert batches", err)),
                            };

                        let _ = s1.send(res).await;
                    }
                    Err(err) => {
                        let _ = s1
                            .send(Err(status!("Could not convert batches", err)))
                            .await;
                    }
                }
            }
            is_finished_clone.store(true, Ordering::SeqCst);
        });

        if is_native_client {
            tokio::spawn(async move {
                let total_scan_value = context.get_total_scan_value();
                let mut current_scan_value = context.get_scan_progress_value();

                let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(20));
                while !is_finished.load(Ordering::SeqCst) {
                    interval.tick().await;

                    let progress = context.get_scan_progress_value();
                    if progress.rows == current_scan_value.rows {
                        continue;
                    }
                    current_scan_value = progress;

                    let progress = ProgressValue {
                        total_rows: total_scan_value.rows,
                        total_bytes: total_scan_value.bytes,

                        read_rows: current_scan_value.rows,
                        read_bytes: current_scan_value.bytes,
                    };

                    let progress = serde_json::to_vec(&progress).unwrap();
                    let progress_flight_data = FlightData {
                        app_metadata: vec![H_PROGRESS].into(),
                        data_body: progress.into(),
                        ..Default::default()
                    };
                    let _ = sender.send(Ok(progress_flight_data)).await;
                }
            });
        }

        fn receiver_to_stream<T>(
            receiver: tokio::sync::mpsc::Receiver<T>,
        ) -> impl Stream<Item = T> {
            futures::stream::unfold(receiver, |mut receiver| async {
                receiver.recv().await.map(|value| (value, receiver))
            })
        }

        let st = receiver_to_stream(receiver);
        Ok(Box::pin(st))
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct ProgressValue {
    pub total_rows: usize,
    pub total_bytes: usize,

    pub read_rows: usize,
    pub read_bytes: usize,
}
