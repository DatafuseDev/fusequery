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
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::HashMethodKind;
use common_functions::BUILTIN_FUNCTIONS;
use common_storage::DataOperator;

use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::hash_join::BuildSpillCoordinator;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::sessions::QueryContext;
use crate::spiller::Spiller;
use crate::spiller::SpillerConfig;
use crate::spiller::SpillerType;

/// Define some states for hash join build spilling
pub struct BuildSpillState {
    /// Hash join build state
    pub build_state: Arc<HashJoinBuildState>,
    /// Spilling memory threshold
    pub spill_memory_threshold: usize,
    /// Hash join build spilling coordinator
    pub spill_coordinator: Arc<BuildSpillCoordinator>,
    /// Spiller, responsible for specific spill work
    pub spiller: Spiller,
}

impl BuildSpillState {
    pub fn create(
        _ctx: Arc<QueryContext>,
        spill_coordinator: Arc<BuildSpillCoordinator>,
        build_state: Arc<HashJoinBuildState>,
    ) -> Self {
        let spill_config = SpillerConfig::create("_hash_join_build_spill".to_string());
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(operator, spill_config, SpillerType::HashJoin);
        Self {
            build_state,
            spill_memory_threshold: 1024,
            spill_coordinator,
            spiller,
        }
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

    // Get all hashes for input data.
    fn get_hashes(&self, block: &DataBlock, hashes: &mut Vec<u64>) -> Result<()> {
        let func_ctx = self.build_state.ctx.get_function_context()?;
        let evaluator = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS);
        // Use the first column as the key column to generate hash
        let first_build_key = &self.build_state.hash_join_state.hash_join_desc.build_keys[0];
        let build_key_column = evaluator.run(first_build_key)?;
        let build_key_column = build_key_column.as_column().unwrap();
        // Todo: simplify the following code
        match &*self.build_state.method {
            HashMethodKind::Serializer(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::DictionarySerializer(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::SingleString(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU8(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU16(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU32(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU64(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU128(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU256(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
        }
        Ok(())
    }

    // Check if need to spill.
    // Notes: even if the method returns false, but there exists one processor need to spill, then it needs to wait spill.
    pub(crate) fn check_need_spill(&self) -> Result<bool> {
        let settings = self.build_state.ctx.get_settings();
        let spill_threshold = settings.get_join_spilling_threshold()?;
        let enable_spill = settings.get_enable_join_spill()? && spill_threshold != 0;
        if !enable_spill || self.spiller.is_all_spilled() {
            return Ok(false);
        }

        // Todo: if this is the first batch data, directly return false.

        let mut total_bytes = 0;

        {
            let _ = self
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

        if total_bytes > spill_threshold {
            return Ok(true);
        }

        Ok(false)
    }

    #[async_backtrace::framed]
    // Directly spill input data without buffering.
    // Return unspilled data.
    pub(crate) async fn spill_input(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        // Save the row index which is not spilled.
        let mut unspilled_row_index = Vec::with_capacity(data_block.num_rows());
        // Compute the hash value for each row.
        let mut hashes = Vec::with_capacity(data_block.num_rows());
        self.get_hashes(&data_block, &mut hashes)?;
        // Key is partition, value is row indexes
        let mut partition_rows = HashMap::new();
        // Classify rows to spill or not spill.
        for (row_idx, hash) in hashes.iter().enumerate() {
            let partition_id = *hash as u8 & 0b0000_0111;
            if self.spiller.spilled_partition_set.contains(&partition_id) {
                // the row can be directly spilled to corresponding partition
                partition_rows
                    .entry(partition_id)
                    .and_modify(|v: &mut Vec<usize>| v.push(row_idx))
                    .or_insert(vec![row_idx]);
            } else {
                unspilled_row_index.push(row_idx);
            }
        }
        for (p_id, row_indexes) in partition_rows.iter() {
            let block_row_indexes = row_indexes
                .iter()
                .map(|idx| (0 as u32, *idx as u32, 1 as usize))
                .collect::<Vec<_>>();
            let block = DataBlock::take_blocks(
                &[data_block.clone()],
                &block_row_indexes,
                row_indexes.len(),
            );
            // Spill block with partition id
            self.spiller.spill_with_partition(p_id, &block).await?;
        }
        // Return unspilled data
        let unspilled_block_row_indexes = unspilled_row_index
            .iter()
            .map(|idx| (0 as u32, *idx as u32, 1 as usize))
            .collect::<Vec<_>>();
        Ok(DataBlock::take_blocks(
            &[data_block.clone()],
            &unspilled_block_row_indexes,
            unspilled_row_index.len(),
        ))
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
        blocks.extend(chunks.drain(..));
        Ok(blocks)
    }

    // Partition input blocks to different partitions.
    // Output is <partition_id, blocks>
    fn partition_input_blocks(
        &self,
        input_blocks: &[DataBlock],
    ) -> Result<HashMap<u8, Vec<DataBlock>>> {
        let mut partition_blocks = HashMap::new();
        let mut partition_map = HashMap::with_capacity(8);
        for block in input_blocks.iter() {
            let mut hashes = Vec::with_capacity(block.num_rows());
            self.get_hashes(block, &mut hashes)?;
            for (row_idx, hash) in hashes.iter().enumerate() {
                let partition_id = *hash as u8 & 0b0000_0111;
                partition_map
                    .entry(partition_id)
                    .and_modify(|v: &mut Vec<usize>| v.push(row_idx))
                    .or_insert(vec![row_idx]);
            }
            for (partition_id, row_indexes) in partition_map.iter() {
                let block_row_indexes = row_indexes
                    .iter()
                    .map(|idx| (0 as u32, *idx as u32, 1 as usize))
                    .collect::<Vec<_>>();
                let block =
                    DataBlock::take_blocks(&[block.clone()], &block_row_indexes, row_indexes.len());
                partition_blocks
                    .entry(*partition_id)
                    .and_modify(|v: &mut Vec<DataBlock>| v.push(block.clone()))
                    .or_insert(vec![block]);
            }
        }
        Ok(partition_blocks)
    }

    // Split all spill tasks equally to all processors
    // Tasks will be sent to `BuildSpillCoordinator`.
    pub(crate) fn split_spill_tasks(&self) -> Result<()> {
        let processors_num = self.spill_coordinator.total_builder_count.read();
        let mut spill_tasks = self.spill_coordinator.spill_tasks.write();
        let blocks = self.collect_rows()?;
        let partition_blocks = self.partition_input_blocks(&blocks)?;
        let mut partition_tasks = HashMap::with_capacity(partition_blocks.len());
        // Stat how many rows in each partition, then split it equally.
        for (partition_id, blocks) in partition_blocks.iter() {
            let merged_block = DataBlock::concat(blocks)?;
            let total_rows = merged_block.num_rows();
            // Equally split blocks to `processors_num` parts
            let mut start_row;
            let mut end_row = 0;
            for i in 0..*processors_num {
                start_row = end_row;
                end_row = start_row + total_rows / *processors_num;
                if i == *processors_num - 1 {
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
        while task_id < *processors_num {
            let mut task = Vec::with_capacity(partition_tasks.len());
            for partition_id in partition_tasks.keys() {
                task.push((
                    *partition_id,
                    partition_tasks.get(partition_id).unwrap()[task_id as usize].clone(),
                ))
            }
            spill_tasks.push_back(task);
            task_id += 1;
        }

        Ok(())
    }
}
