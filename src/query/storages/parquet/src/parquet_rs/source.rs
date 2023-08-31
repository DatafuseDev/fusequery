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

use std::any::Any;
use std::sync::Arc;

use common_base::base::Progress;
use common_base::base::ProgressValues;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

use super::parquet_reader::ParquetRSReader;
use crate::ParquetPart;

enum State {
    Init,
    ReadRowGroup(ParquetRecordBatchReader),
    ReadFiles(Vec<Vec<u8>>),
}

pub struct ParquetSource {
    // Source processor related fields.
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,

    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    // Used to read parquet.
    reader: Arc<ParquetRSReader>,

    state: State,
}

impl ParquetSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        reader: Arc<ParquetRSReader>,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            scan_progress,
            ctx,
            reader,
            generated_data: None,
            is_finished: false,
            state: State::Init,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for ParquetSource {
    fn name(&self) -> String {
        "ParquetRSSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        match self.generated_data.take() {
            None => match &self.state {
                State::Init => Ok(Event::Async),
                State::ReadFiles(_) => Ok(Event::Sync),
                State::ReadRowGroup(_) => Ok(Event::Sync),
            },
            Some(data_block) => {
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Init) {
            State::ReadRowGroup(mut reader) => {
                if let Some(block) = self.reader.read_block(&mut reader)? {
                    self.generated_data = Some(block);
                    self.state = State::ReadRowGroup(reader);
                }
                // Else: The reader is finished. We should try to build another reader.
            }
            State::ReadFiles(buffers) => {
                let mut blocks = Vec::with_capacity(buffers.len());
                for buffer in buffers {
                    blocks.extend(self.reader.read_blocks_from_binary(buffer)?);
                }
                if !blocks.is_empty() {
                    self.generated_data = Some(DataBlock::concat(&blocks)?);
                }
                // Else: no output data is generated.
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Init) {
            State::Init => {
                if let Some(part) = self.ctx.get_partition() {
                    match ParquetPart::from_part(&part)? {
                        ParquetPart::ParquetRSRowGroup(part) => {
                            if let Some(reader) = self.reader.prepare_row_group_reader(part).await?
                            {
                                self.state = State::ReadRowGroup(reader);
                            }
                            // Else: keep in init state.
                        }
                        ParquetPart::ParquetFiles(parts) => {
                            let mut handlers = Vec::with_capacity(parts.files.len());
                            for (path, _) in parts.files.iter() {
                                let op = self.reader.operator();
                                let path = path.clone();
                                handlers.push(async move { op.read(path.as_str()).await });
                            }
                            let buffers = futures::future::try_join_all(handlers).await?;
                            self.state = State::ReadFiles(buffers);
                        }
                        _ => unreachable!(),
                    }
                } else {
                    self.is_finished = true;
                }
            }
            _ => unreachable!(),
        }

        Ok(())
    }
}
