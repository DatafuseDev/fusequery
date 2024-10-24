// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use async_channel::Sender;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_sql::field_default_value;
use databend_common_storage::ColumnNodes;

use crate::pruning_pipeline::meta_info::BlockPruningResult;
use crate::FuseTable;

pub struct SendPartitionSink {
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    push_downs: Option<PushDownInfo>,
    is_native: bool,
    sender: Sender<Partitions>,
    index: usize,
}

impl SendPartitionSink {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        push_downs: Option<PushDownInfo>,
        is_native: bool,
        sender: Sender<Partitions>,
        index: usize,
        input: Arc<InputPort>,
    ) -> databend_common_exception::Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncSinker::create(
            input,
            SendPartitionSink {
                ctx,
                schema,
                push_downs,
                is_native,
                sender,
                index,
            },
        )))
    }
}

#[async_trait::async_trait]
impl AsyncSink for SendPartitionSink {
    const NAME: &'static str = "SendPartitionSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> databend_common_exception::Result<()> {
        self.sender.close();
        Ok(())
    }

    async fn consume(&mut self, data_block: DataBlock) -> databend_common_exception::Result<bool> {
        if let Some(info_ptr) = data_block.get_meta() {
            if let Some(data) = BlockPruningResult::downcast_ref_from(info_ptr) {
                let arrow_schema = self.schema.as_ref().into();
                let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));
                let top_k = self
                    .push_downs
                    .as_ref()
                    .filter(|_| self.is_native) // Only native format supports topk push down.
                    .and_then(|p| p.top_k(self.schema.as_ref()))
                    .map(|topk| {
                        field_default_value(self.ctx.clone(), &topk.field).map(|d| (topk, d))
                    })
                    .transpose()?;

                let (_statistics, parts) = FuseTable::to_partitions(
                    Some(&self.schema),
                    &data.block_metas,
                    &column_nodes,
                    top_k,
                    self.push_downs.clone(),
                );
                self.sender.send(parts).await.map_err(|_err| {
                    ErrorCode::Internal(format!(
                        "SendPartitionSink send data failed: index {:?}",
                        self.index
                    ))
                })?;
                return Ok(false);
            }
        }
        Err(ErrorCode::Internal(
            "SendPartitionSink get wrong data block",
        ))
    }
}
