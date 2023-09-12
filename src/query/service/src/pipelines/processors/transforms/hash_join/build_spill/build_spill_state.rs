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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::HashMethod;
use common_expression::HashMethodKind;
use common_functions::BUILTIN_FUNCTIONS;
use common_hashtable::hash2bucket;
use common_pipeline_core::query_spill_prefix;
use common_sql::plans::JoinType;
use common_storage::DataOperator;

use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::hash_join::spill_common::get_hashes;
use crate::pipelines::processors::transforms::hash_join::BuildSpillCoordinator;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::sessions::QueryContext;
use crate::spillers::Spiller;
use crate::spillers::SpillerConfig;
use crate::spillers::SpillerType;

/// Define some states for hash join build spilling
pub struct BuildSpillState {
    /// Hash join build state
    pub build_state: Arc<HashJoinBuildState>,
    /// Hash join build spilling coordinator
    pub spill_coordinator: Arc<BuildSpillCoordinator>,
    /// Spiller, responsible for specific spill work
    pub spiller: Spiller,
}

impl BuildSpillState {
    pub fn create(
        ctx: Arc<QueryContext>,
        spill_coordinator: Arc<BuildSpillCoordinator>,
        build_state: Arc<HashJoinBuildState>,
    ) -> Self {
        let tenant = ctx.get_tenant();
        let spill_config = SpillerConfig::create(query_spill_prefix(&tenant));
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(operator, spill_config, SpillerType::HashJoinBuild);
        Self {
            build_state,
            spill_coordinator,
            spiller,
        }
    }

    // Get all hashes for build input data.
    pub fn get_hashes(&self, block: &DataBlock, hashes: &mut Vec<u64>) -> Result<()> {
        let func_ctx = self.build_state.ctx.get_function_context()?;
        let keys = &self.build_state.hash_join_state.hash_join_desc.build_keys;
        get_hashes(&func_ctx, block, keys, &*self.build_state.method, hashes)
    }

    // Collect all buffered data in `RowSpace` and `Chunks`
    // The method will be executed by only one processor.
    fn collect_rows(&self) -> Result<Vec<DataBlock>> {
        let mut blocks = vec![];
        // Collect rows in `RowSpace`'s buffer
        let mut row_space_buffer = self.build_state.hash_join_state.row_space.buffer.write();
        blocks.extend(row_space_buffer.drain(..));
        let mut buffer_row_size = self
            .build_state
            .hash_join_state
            .row_space
            .buffer_row_size
            .write();
        *buffer_row_size = 0;

        // Collect rows in `Chunks`
        let chunks = unsafe { &mut *self.build_state.hash_join_state.chunks.get() };
        blocks.append(chunks);
        Ok(blocks)
    }

    // Partition input blocks to different partitions.
    // Output is <partition_id, blocks>
    fn partition_input_blocks(
        &self,
        input_blocks: &[DataBlock],
    ) -> Result<HashMap<u8, Vec<DataBlock>>> {
        let mut partition_blocks = HashMap::new();
        for block in input_blocks.iter() {
            let mut hashes = Vec::with_capacity(block.num_rows());
            self.get_hashes(block, &mut hashes)?;
            let mut indices = Vec::with_capacity(hashes.len());
            for hash in hashes {
                indices.push(hash2bucket::<2, true>(hash as usize) as u8);
            }
            let scatter_blocks = DataBlock::scatter(block, &indices, 1 << 2)?;
            for (p_id, p_block) in scatter_blocks.into_iter().enumerate() {
                partition_blocks
                    .entry(p_id as u8)
                    .and_modify(|v: &mut Vec<DataBlock>| v.push(p_block.clone()))
                    .or_insert(vec![p_block]);
            }
        }
        Ok(partition_blocks)
    }
}

/// Define some spill-related APIs for hash join build
impl BuildSpillState {
    #[async_backtrace::framed]
    // Start to spill, get the processor's spill task from `BuildSpillCoordinator`
    pub(crate) async fn spill(&mut self) -> Result<()> {
        let spill_partitions = {
            let mut spill_tasks = self.spill_coordinator.spill_tasks.write();
            spill_tasks.pop_back().unwrap()
        };
        self.spiller.spill(&spill_partitions).await
    }

    // Check if need to spill.
    // Notes: even if the method returns false, but there exists one processor need to spill, then it needs to wait spill.
    pub(crate) fn check_need_spill(&self) -> Result<bool> {
        let settings = self.build_state.ctx.get_settings();
        let spill_threshold = settings.get_join_spilling_threshold()?;
        // If `spill_threshold` is 0, we won't limit memory.
        let enable_spill = settings.get_enable_join_spill()?
            && spill_threshold != 0
            && self.build_state.hash_join_state.hash_join_desc.join_type == JoinType::Inner;
        if !enable_spill || self.spiller.is_all_spilled() {
            return Ok(false);
        }

        // Todo: if this is the first batch data, directly return false.

        let mut total_bytes = 0;

        {
            let _lock = self
                .build_state
                .hash_join_state
                .row_space
                .write_lock
                .write();
            for block in self
                .build_state
                .hash_join_state
                .row_space
                .buffer
                .read()
                .iter()
            {
                total_bytes += block.memory_size();
            }

            let chunks = unsafe { &*self.build_state.hash_join_state.chunks.get() };
            for block in chunks.iter() {
                total_bytes += block.memory_size();
            }

            if total_bytes > spill_threshold {
                return Ok(true);
            }
        }
        Ok(false)
    }

    // Split all spill tasks equally to all processors
    // Tasks will be sent to `BuildSpillCoordinator`.
    pub(crate) fn split_spill_tasks(&self) -> Result<()> {
        let active_processors_num = self.spill_coordinator.total_builder_count
            - *self.spill_coordinator.non_spill_processors.read();
        let mut spill_tasks = self.spill_coordinator.spill_tasks.write();
        let blocks = self.collect_rows()?;
        let partition_blocks = self.partition_input_blocks(&blocks)?;
        let mut partition_tasks = HashMap::with_capacity(partition_blocks.len());
        // Stat how many rows in each partition, then split it equally.
        for (partition_id, blocks) in partition_blocks.iter() {
            let merged_block = DataBlock::concat(blocks)?;
            let total_rows = merged_block.num_rows();
            // Equally split blocks to `active_processors_num` parts
            let mut start_row;
            let mut end_row = 0;
            for i in 0..active_processors_num {
                start_row = end_row;
                end_row = start_row + total_rows / active_processors_num;
                if i == active_processors_num - 1 {
                    end_row = total_rows;
                }
                let sub_block = merged_block.slice(start_row..end_row);
                partition_tasks
                    .entry(*partition_id)
                    .and_modify(|v: &mut Vec<DataBlock>| v.push(sub_block.clone()))
                    .or_insert(vec![sub_block]);
            }
        }

        // Todo: we don't need to spill all partitions.
        let mut task_id: usize = 0;
        while task_id < active_processors_num {
            let mut task = Vec::with_capacity(partition_tasks.len());
            for partition_id in partition_tasks.keys() {
                task.push((
                    *partition_id,
                    partition_tasks.get(partition_id).unwrap()[task_id].clone(),
                ))
            }
            spill_tasks.push_back(task);
            task_id += 1;
        }

        Ok(())
    }
}
