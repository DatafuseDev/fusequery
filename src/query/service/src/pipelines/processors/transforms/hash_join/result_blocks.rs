// Copyright 2022 Datafuse Labs.
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

use std::iter::TrustedLen;

use common_arrow::arrow::bitmap::Bitmap;
use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::combine_validities_3;
use common_datavalues::wrap_nullable;
use common_datavalues::BooleanColumn;
use common_datavalues::BooleanType;
use common_datavalues::BooleanViewer;
use common_datavalues::Column;
use common_datavalues::ColumnRef;
use common_datavalues::ConstColumn;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_datavalues::NullableColumn;
use common_datavalues::NullableType;
use common_datavalues::ScalarViewer;
use common_datavalues::Series;
use common_exception::ErrorCode;
use common_exception::Result;
use common_hashtable::HashMap;
use common_hashtable::HashTableKeyable;
use common_hashtable::KeyValueEntity;

use super::JoinHashTable;
use super::ProbeState;
use crate::evaluator::EvalNode;
use crate::pipelines::processors::transforms::hash_join::join_hash_table::MarkerKind;
use crate::pipelines::processors::transforms::hash_join::row::RowPtr;
use crate::sessions::TableContext;
use crate::sql::planner::plans::JoinType;

impl JoinHashTable {
    pub(crate) fn result_blocks<Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<Vec<DataBlock>>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;

        let mut results: Vec<DataBlock> = vec![];
        match self.hash_join_desc.join_type {
            JoinType::Inner => {
                for (i, key) in keys_iter.enumerate() {
                    // If the join is derived from correlated subquery, then null equality is safe.
                    let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                        hash_table.find_key(&key)
                    } else {
                        Self::probe_key(hash_table, key, valids, i)
                    };
                    match probe_result_ptr {
                        Some(v) => {
                            let probe_result_ptrs = v.get_value();
                            build_indexs.extend_from_slice(probe_result_ptrs);

                            for _ in probe_result_ptrs {
                                probe_indexs.push(i as u32);
                            }
                        }
                        None => continue,
                    }
                }

                let build_block = self.row_space.gather(build_indexs)?;
                let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
                let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

                match &self.hash_join_desc.other_predicate {
                    Some(other_predicate) => {
                        let func_ctx = self.ctx.try_get_function_context()?;
                        let filter_vector = other_predicate.eval(&func_ctx, &merged_block)?;
                        results.push(DataBlock::filter_block(
                            merged_block,
                            filter_vector.vector(),
                        )?);
                    }
                    None => results.push(merged_block),
                }
            }
            JoinType::LeftSemi => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result = self.left_semi_anti_join::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                } else {
                    let result = self.left_semi_anti_join_with_other_conjunct::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }
            JoinType::LeftAnti => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result = self.left_semi_anti_join::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                } else {
                    let result = self.left_semi_anti_join_with_other_conjunct::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }
            JoinType::RightSemi | JoinType::RightAnti => {
                let result = self.right_join::<_, _>(hash_table, probe_state, keys_iter, input)?;
                return Ok(vec![result]);
            }
            // Single join is similar to left join, but the result is a single row.
            JoinType::Left | JoinType::Single | JoinType::Full => {
                if self.hash_join_desc.other_predicate.is_none() {
                    let result = self.left_or_single_join::<false, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                } else {
                    let result = self.left_or_single_join::<true, _, _>(
                        hash_table,
                        probe_state,
                        keys_iter,
                        input,
                    )?;
                    return Ok(vec![result]);
                }
            }
            JoinType::Right => {
                let result = self.right_join::<_, _>(hash_table, probe_state, keys_iter, input)?;
                return Ok(vec![result]);
            }
            // Three cases will produce Mark join:
            // 1. uncorrelated ANY subquery: only have one kind of join condition, equi-condition or non-equi-condition.
            // 2. correlated ANY subquery: must have two kinds of join condition, one is equi-condition and the other is non-equi-condition.
            //    equi-condition is subquery's outer columns with subquery's derived columns.
            //    non-equi-condition is subquery's child expr with subquery's output column.
            //    for example: select * from t1 where t1.a = ANY (select t2.a from t2 where t2.b = t1.b); [t1: a, b], [t2: a, b]
            //    subquery's outer columns: t1.b, and it'll derive a new column: subquery_5 when subquery cross join t1;
            //    so equi-condition is t1.b = subquery_5, and non-equi-condition is t1.a = t2.a.
            // 3. Correlated Exists subquery： only have one kind of join condition, equi-condition.
            //    equi-condition is subquery's outer columns with subquery's derived columns. (see the above example in correlated ANY subquery)
            JoinType::LeftMark => {
                results.push(DataBlock::empty());
                self.left_mark_join(hash_table, probe_state, keys_iter, input)?;
            }
            JoinType::RightMark => {
                let result = self.right_mark_join(hash_table, probe_state, keys_iter, input)?;
                return Ok(vec![result]);
            }
            _ => {
                return Err(ErrorCode::UnImplement(format!(
                    "{} is unimplemented",
                    self.hash_join_desc.join_type
                )));
            }
        }
        Ok(results)
    }

    fn left_mark_join<Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<()>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        // `probe_column` is the subquery result column.
        // For sql: select * from t1 where t1.a in (select t2.a from t2); t2.a is the `probe_column`,
        let probe_column = input.column(0);
        // Check if there is any null in the probe column.
        if let Some(validity) = probe_column.validity().1 {
            if validity.unset_bits() > 0 {
                let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
                *has_null = true;
            }
        }
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };
            if let Some(v) = probe_result_ptr {
                let probe_result_ptrs = v.get_value();
                build_indexs.extend_from_slice(probe_result_ptrs);
                probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));
                for ptr in probe_result_ptrs {
                    // If has other conditions, we'll process marker later
                    if self.hash_join_desc.other_predicate.is_none() {
                        // If find join partner, set the marker to true.
                        let mut self_row_ptrs = self.row_ptrs.write();
                        if let Some(p) = self_row_ptrs.iter_mut().find(|p| (*p).eq(&ptr)) {
                            p.marker = Some(MarkerKind::True);
                        }
                    }
                }
            }
        }
        if self.hash_join_desc.other_predicate.is_none() {
            return Ok(());
        }

        if self.hash_join_desc.from_correlated_subquery {
            // Must be correlated ANY subquery, we won't need to check `has_null` in `mark_join_blocks`.
            // In the following, if value is Null and Marker is False, we'll set the marker to Null
            let mut has_null = self.hash_join_desc.marker_join_desc.has_null.write();
            *has_null = false;
        }
        let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        let build_block = self.row_space.gather(build_indexs)?;
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;
        let func_ctx = self.ctx.try_get_function_context()?;
        let type_vector = self
            .hash_join_desc
            .other_predicate
            .as_ref()
            .unwrap()
            .eval(&func_ctx, &merged_block)?;
        let filter_column = type_vector.vector();
        let boolean_viewer = BooleanViewer::try_create(filter_column)?;
        let mut row_ptrs = self.row_ptrs.write();
        for (idx, build_index) in build_indexs.iter().enumerate() {
            let self_row_ptr = row_ptrs.iter_mut().find(|p| (*p).eq(&build_index)).unwrap();
            if !boolean_viewer.valid_at(idx) {
                if self_row_ptr.marker == Some(MarkerKind::False) {
                    self_row_ptr.marker = Some(MarkerKind::Null);
                }
            } else if boolean_viewer.value_at(idx) {
                self_row_ptr.marker = Some(MarkerKind::True);
            }
        }
        Ok(())
    }

    pub(crate) fn create_marker_block(
        &self,
        has_null: bool,
        markers: Vec<MarkerKind>,
    ) -> Result<DataBlock> {
        let mut validity = MutableBitmap::with_capacity(markers.len());
        let mut boolean_bit_map = MutableBitmap::with_capacity(markers.len());

        for m in markers {
            let marker = if m == MarkerKind::False && has_null {
                MarkerKind::Null
            } else {
                m
            };
            if marker == MarkerKind::Null {
                validity.push(false);
            } else {
                validity.push(true);
            }
            if marker == MarkerKind::True {
                boolean_bit_map.push(true);
            } else {
                boolean_bit_map.push(false);
            }
        }
        let boolean_column = BooleanColumn::from_arrow_data(boolean_bit_map.into());
        let marker_column = Self::set_validity(&boolean_column.arc(), &validity.into())?;
        let marker_schema = DataSchema::new(vec![DataField::new(
            &self
                .hash_join_desc
                .marker_join_desc
                .marker_index
                .ok_or_else(|| ErrorCode::LogicalError("Invalid mark join"))?
                .to_string(),
            NullableType::new_impl(BooleanType::new_impl()),
        )]);
        Ok(DataBlock::create(DataSchemaRef::from(marker_schema), vec![
            marker_column,
        ]))
    }

    pub(crate) fn init_markers(cols: &[ColumnRef], num_rows: usize) -> Vec<MarkerKind> {
        let mut markers = vec![MarkerKind::False; num_rows];
        if cols.iter().any(|c| c.is_nullable() || c.is_null()) {
            let mut valids = None;
            for col in cols.iter() {
                let (is_all_null, tmp_valids_option) = col.validity();
                if !is_all_null {
                    if let Some(tmp_valids) = tmp_valids_option.as_ref() {
                        if tmp_valids.unset_bits() == 0 {
                            let mut m = MutableBitmap::with_capacity(num_rows);
                            m.extend_constant(num_rows, true);
                            valids = Some(m.into());
                            break;
                        } else {
                            valids = combine_validities_3(valids, tmp_valids_option.cloned());
                        }
                    }
                }
            }
            if let Some(v) = valids {
                for (idx, marker) in markers.iter_mut().enumerate() {
                    if !v.get_bit(idx) {
                        *marker = MarkerKind::Null;
                    }
                }
            }
        }
        markers
    }

    fn right_mark_join<Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let has_null = {
            let has_null_ref = self.hash_join_desc.marker_join_desc.has_null.read();
            *has_null_ref
        };

        let mut markers = Self::init_markers(input.columns(), input.num_rows());
        let valids = &probe_state.valids;
        if self.hash_join_desc.other_predicate.is_none() {
            // todo(youngsofun): can be optimized as semi-join?
            for (i, key) in keys_iter.enumerate() {
                let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                    hash_table.find_key(&key)
                } else {
                    Self::probe_key(hash_table, key, valids, i)
                };
                if probe_result_ptr.is_some() {
                    markers[i] = MarkerKind::True;
                }
            }
        } else {
            let mut probe_indexes = vec![];
            let mut build_indexes = vec![];
            for (i, key) in keys_iter.enumerate() {
                let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                    hash_table.find_key(&key)
                } else {
                    Self::probe_key(hash_table, key, valids, i)
                };
                if let Some(v) = probe_result_ptr {
                    let probe_result_ptrs = v.get_value();
                    build_indexes.extend_from_slice(probe_result_ptrs);
                    probe_indexes.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));
                }
            }

            let probe_block = DataBlock::block_take_by_indices(input, &probe_indexes)?;
            let build_block = self.row_space.gather(&build_indexes)?;
            let merged_block = self.merge_eq_block(&build_block, &probe_block)?;
            let func_ctx = self.ctx.try_get_function_context()?;
            let type_vector = self
                .hash_join_desc
                .other_predicate
                .as_ref()
                .unwrap()
                .eval(&func_ctx, &merged_block)?;
            let filter_column = type_vector.vector();
            let boolean_viewer = BooleanViewer::try_create(filter_column)?;
            for idx in 0..boolean_viewer.len() {
                let marker = &mut markers[probe_indexes[idx] as usize];
                if !boolean_viewer.valid_at(idx) {
                    if *marker == MarkerKind::False {
                        *marker = MarkerKind::Null;
                    }
                } else if boolean_viewer.value_at(idx) {
                    *marker = MarkerKind::True;
                }
            }
        }

        let marker_block = self.create_marker_block(has_null, markers)?;
        let merged_block = self.merge_eq_block(&marker_block, input)?;
        Ok(merged_block)
    }

    fn left_semi_anti_join<const SEMI: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let valids = &probe_state.valids;

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };

            match (probe_result_ptr, SEMI) {
                (Some(_), true) | (None, false) => {
                    probe_indexs.push(i as u32);
                }
                _ => {}
            }
        }
        DataBlock::block_take_by_indices(input, probe_indexs)
    }

    fn left_semi_anti_join_with_other_conjunct<const SEMI: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let build_indexs = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;
        let row_state = &mut probe_state.row_state;

        // For semi join, it defaults to all
        row_state.resize(keys_iter.size_hint().0, 0);

        let mut dummys = 0;

        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };

            match (probe_result_ptr, SEMI) {
                (Some(v), _) => {
                    let probe_result_ptrs = v.get_value();
                    build_indexs.extend_from_slice(probe_result_ptrs);
                    probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));

                    if !SEMI {
                        row_state[i] += probe_result_ptrs.len() as u32;
                    }
                }

                (None, false) => {
                    // dummy row ptr
                    build_indexs.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                        marker: None,
                    });
                    probe_indexs.push(i as u32);

                    dummys += 1;
                    // must not be filtered out， so we should not increase the row_state for anti join
                    // row_state[i] += 1;
                }
                _ => {}
            }
        }
        let probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        // faster path for anti join
        if dummys == probe_indexs.len() {
            return Ok(probe_block);
        }

        let build_block = self.row_space.gather(build_indexs)?;
        let merged_block = self.merge_eq_block(&build_block, &probe_block)?;

        let (bm, all_true, all_false) = self.get_other_filters(
            &merged_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        let mut bm = match (bm, all_true, all_false) {
            (Some(b), _, _) => b.into_mut().right().unwrap(),
            (_, true, _) => MutableBitmap::from_len_set(merged_block.num_rows()),
            (_, _, true) => MutableBitmap::from_len_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };

        if SEMI {
            Self::fill_null_for_semi_join(&mut bm, probe_indexs, row_state);
        } else {
            Self::fill_null_for_anti_join(&mut bm, probe_indexs, row_state);
        }

        let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
        DataBlock::filter_block(probe_block, &predicate)
    }

    fn left_or_single_join<const WITH_OTHER_CONJUNCT: bool, Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let probe_indexs = &mut probe_state.probe_indexs;
        let local_build_indexes = &mut probe_state.build_indexs;
        let valids = &probe_state.valids;

        let row_state = &mut probe_state.row_state;

        if WITH_OTHER_CONJUNCT {
            row_state.resize(keys_iter.size_hint().0, 0);
        }

        let mut validity = MutableBitmap::new();
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = if self.hash_join_desc.from_correlated_subquery {
                hash_table.find_key(&key)
            } else {
                Self::probe_key(hash_table, key, valids, i)
            };

            match probe_result_ptr {
                Some(v) => {
                    let probe_result_ptrs = v.get_value();
                    if self.hash_join_desc.join_type == JoinType::Full {
                        let mut build_indexes =
                            self.hash_join_desc.right_join_desc.build_indexes.write();
                        build_indexes.extend(probe_result_ptrs);
                    }
                    if self.hash_join_desc.join_type == JoinType::Single
                        && probe_result_ptrs.len() > 1
                    {
                        return Err(ErrorCode::LogicalError(
                            "Scalar subquery can't return more than one row",
                        ));
                    }
                    local_build_indexes.extend_from_slice(probe_result_ptrs);
                    probe_indexs.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));

                    if WITH_OTHER_CONJUNCT {
                        row_state[i] += probe_result_ptrs.len() as u32;
                    }
                    validity.extend_constant(probe_result_ptrs.len(), true);
                }
                None => {
                    if self.hash_join_desc.join_type == JoinType::Full {
                        let mut build_indexes =
                            self.hash_join_desc.right_join_desc.build_indexes.write();
                        // dummy row ptr
                        // here assume there is no RowPtr, which chunk_index is u32::MAX and row_index is u32::MAX
                        build_indexes.push(RowPtr {
                            chunk_index: u32::MAX,
                            row_index: u32::MAX,
                            marker: None,
                        });
                    }
                    // dummy row ptr
                    local_build_indexes.push(RowPtr {
                        chunk_index: 0,
                        row_index: 0,
                        marker: None,
                    });
                    probe_indexs.push(i as u32);
                    validity.push(false);

                    if WITH_OTHER_CONJUNCT {
                        row_state[i] += 1;
                    }
                }
            }
        }

        let validity: Bitmap = validity.into();
        let build_block = if !self.hash_join_desc.from_correlated_subquery
            && self.hash_join_desc.join_type == JoinType::Single
            && validity.unset_bits() == input.num_rows()
        {
            // Uncorrelated scalar subquery and no row was returned by subquery
            // We just construct a block with NULLs
            let build_data_schema = self.row_space.data_schema.clone();
            let columns = build_data_schema
                .fields()
                .iter()
                .map(|field| {
                    let data_type = wrap_nullable(field.data_type());
                    data_type.create_column(&vec![DataValue::Null; input.num_rows()])
                })
                .collect::<Result<Vec<ColumnRef>>>()?;
            DataBlock::create(build_data_schema, columns)
        } else {
            self.row_space.gather(local_build_indexes)?
        };

        let nullable_columns =
            if self.row_space.datablocks().is_empty() && !local_build_indexes.is_empty() {
                build_block
                    .columns()
                    .iter()
                    .map(|c| {
                        c.data_type()
                            .create_constant_column(&DataValue::Null, local_build_indexes.len())
                    })
                    .collect::<Result<Vec<_>>>()?
            } else {
                build_block
                    .columns()
                    .iter()
                    .map(|c| Self::set_validity(c, &validity))
                    .collect::<Result<Vec<_>>>()?
            };
        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns.clone());
        let mut probe_block = DataBlock::block_take_by_indices(input, probe_indexs)?;
        if self.hash_join_desc.join_type == JoinType::Full {
            let nullable_probe_columns = probe_block
                .columns()
                .iter()
                .map(|c| {
                    let mut probe_validity = MutableBitmap::new();
                    probe_validity.extend_constant(c.len(), true);
                    let probe_validity: Bitmap = probe_validity.into();
                    Self::set_validity(c, &probe_validity)
                })
                .collect::<Result<Vec<_>>>()?;
            probe_block = DataBlock::create(self.probe_schema.clone(), nullable_probe_columns);
        }
        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        if !WITH_OTHER_CONJUNCT {
            return Ok(merged_block);
        }

        let (bm, all_true, all_false) = self.get_other_filters(
            &merged_block,
            self.hash_join_desc.other_predicate.as_ref().unwrap(),
        )?;

        if all_true {
            return Ok(merged_block);
        }

        let validity = match (bm, all_false) {
            (Some(b), _) => b,
            (None, true) => Bitmap::new_zeroed(merged_block.num_rows()),
            // must be one of above
            _ => unreachable!(),
        };

        let nullable_columns = nullable_columns
            .iter()
            .map(|c| Self::set_validity(c, &validity))
            .collect::<Result<Vec<_>>>()?;
        let nullable_build_block =
            DataBlock::create(self.row_space.data_schema.clone(), nullable_columns.clone());
        let merged_block = self.merge_eq_block(&nullable_build_block, &probe_block)?;

        let mut bm = validity.into_mut().right().unwrap();

        if self.hash_join_desc.join_type == JoinType::Full {
            let mut build_indexes = self.hash_join_desc.right_join_desc.build_indexes.write();
            for (idx, build_index) in build_indexes.iter_mut().enumerate() {
                if !bm.get(idx) {
                    build_index.marker = Some(MarkerKind::False);
                }
            }
        }
        Self::fill_null_for_left_join(&mut bm, probe_indexs, row_state);

        let predicate = BooleanColumn::from_arrow_data(bm.into()).arc();
        DataBlock::filter_block(merged_block, &predicate)
    }

    fn right_join<Key, IT>(
        &self,
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        probe_state: &mut ProbeState,
        keys_iter: IT,
        input: &DataBlock,
    ) -> Result<DataBlock>
    where
        Key: HashTableKeyable + Clone + 'static,
        IT: Iterator<Item = Key> + TrustedLen,
    {
        let local_build_indexes = &mut probe_state.build_indexs;
        let probe_indexes = &mut probe_state.probe_indexs;
        let valids = &probe_state.valids;
        let mut validity = MutableBitmap::new();
        let mut build_indexes = self.hash_join_desc.right_join_desc.build_indexes.write();
        for (i, key) in keys_iter.enumerate() {
            let probe_result_ptr = Self::probe_key(hash_table, key, valids, i);
            if let Some(v) = probe_result_ptr {
                let probe_result_ptrs = v.get_value();
                build_indexes.extend(probe_result_ptrs);
                local_build_indexes.extend_from_slice(probe_result_ptrs);
                for row_ptr in probe_result_ptrs.iter() {
                    {
                        let mut row_state = self.hash_join_desc.right_join_desc.row_state.write();
                        row_state
                            .entry(*row_ptr)
                            .and_modify(|e| *e += 1)
                            .or_insert(1_usize);
                    }
                }
                probe_indexes.extend(std::iter::repeat(i as u32).take(probe_result_ptrs.len()));
                validity.extend_constant(probe_result_ptrs.len(), true);
            }
        }

        let build_block = self.row_space.gather(local_build_indexes)?;
        let mut probe_block = DataBlock::block_take_by_indices(input, probe_indexes)?;
        // If join type is right join, need to wrap nullable for probe side
        // If join type is semi/anti right join, directly merge `build_block` and `probe_block`
        if self.hash_join_desc.join_type == JoinType::Right {
            let validity: Bitmap = validity.into();
            let nullable_columns = probe_block
                .columns()
                .iter()
                .map(|c| Self::set_validity(c, &validity))
                .collect::<Result<Vec<_>>>()?;
            probe_block = DataBlock::create(self.probe_schema.clone(), nullable_columns);
        }

        self.merge_eq_block(&build_block, &probe_block)
    }

    // modify the bm by the value row_state
    // keep the index of the first positive state
    // bitmap: [1, 1, 1] with row_state [0, 0], probe_index: [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [1, 1, 1] -> [1, 0, 1] -> [1, 0, 0]
    // row_state will be [0, 0] -> [1, 0] -> [1,0] -> [1, 0]
    fn fill_null_for_semi_join(
        bm: &mut MutableBitmap,
        probe_indexs: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexs.iter().enumerate() {
            let row = *row as usize;
            if bm.get(index) {
                if row_state[row] == 0 {
                    row_state[row] = 1;
                } else {
                    bm.set(index, false);
                }
            }
        }
    }

    // keep the index of the negative state
    // bitmap: [1, 1, 1] with row_state [3, 0], probe_index: [0, 0, 0] (repeat the first element 3 times)
    // bitmap will be [1, 1, 1] -> [0, 1, 1] -> [0, 0, 1] -> [0, 0, 0]
    // row_state will be [3, 0] -> [3, 0] -> [3, 0] -> [3, 0]
    fn fill_null_for_anti_join(
        bm: &mut MutableBitmap,
        probe_indexs: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexs.iter().enumerate() {
            let row = *row as usize;
            if row_state[row] == 0 {
                // if state is not matched, anti result will take one
                bm.set(index, true);
            } else if row_state[row] == 1 {
                // if state has just one, anti reverse the result
                row_state[row] -= 1;
                bm.set(index, !bm.get(index))
            } else if !bm.get(index) {
                row_state[row] -= 1;
            } else {
                bm.set(index, false);
            }
        }
    }

    // keep at least one index of the positive state and the null state
    // bitmap: [1, 0, 1] with row_state [2, 1], probe_index: [0, 0, 1]
    // bitmap will be [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1] -> [1, 0, 1]
    // row_state will be [2, 1] -> [2, 1] -> [1, 1] -> [1, 1]
    fn fill_null_for_left_join(
        bm: &mut MutableBitmap,
        probe_indexs: &[u32],
        row_state: &mut [u32],
    ) {
        for (index, row) in probe_indexs.iter().enumerate() {
            let row = *row as usize;
            if row_state[row] == 0 {
                bm.set(index, true);
                continue;
            }

            if row_state[row] == 1 {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state[row] -= 1;
            }
        }
    }

    pub(crate) fn filter_rows_for_right_join(
        bm: &mut MutableBitmap,
        build_indexes: &[RowPtr],
        row_state: &mut std::collections::HashMap<RowPtr, usize>,
    ) {
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] == 1 || row_state[row] == 0 {
                if !bm.get(index) {
                    bm.set(index, true)
                }
                continue;
            }

            if !bm.get(index) {
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
    }

    pub(crate) fn filter_rows_for_right_semi_join(
        &self,
        bm: &mut MutableBitmap,
        build_indexes: &[RowPtr],
        input: DataBlock,
    ) -> Result<DataBlock> {
        let mut row_state = self.hash_join_desc.right_join_desc.row_state.write();
        for (index, row) in build_indexes.iter().enumerate() {
            if row_state[row] > 1 && bm.get(index) {
                bm.set(index, false);
                row_state.entry(*row).and_modify(|e| *e -= 1);
            }
        }
        let predicate = BooleanColumn::from_arrow_data(bm.clone().into()).arc();
        DataBlock::filter_block(input, &predicate)
    }

    // return an (option bitmap, all_true, all_false)
    pub(crate) fn get_other_filters(
        &self,
        merged_block: &DataBlock,
        filter: &EvalNode,
    ) -> Result<(Option<Bitmap>, bool, bool)> {
        let func_ctx = self.ctx.try_get_function_context()?;
        // `predicate_column` contains a column, which is a boolean column.
        let filter_vector = filter.eval(&func_ctx, merged_block)?;
        let predict_boolean_nonull = DataBlock::cast_to_nonull_boolean(filter_vector.vector())?;

        // faster path for constant filter
        if predict_boolean_nonull.is_const() {
            let v = predict_boolean_nonull.get_bool(0)?;
            return Ok((None, v, !v));
        }

        let boolean_col: &BooleanColumn = Series::check_get(&predict_boolean_nonull)?;
        let rows = boolean_col.len();
        let count_zeros = boolean_col.values().unset_bits();

        Ok((
            Some(boolean_col.values().clone()),
            count_zeros == 0,
            rows == count_zeros,
        ))
    }

    pub(crate) fn set_validity(column: &ColumnRef, validity: &Bitmap) -> Result<ColumnRef> {
        if column.is_null() {
            Ok(column.clone())
        } else if column.is_const() {
            let col: &ConstColumn = Series::check_get(column)?;
            let validity = validity.clone();
            let inner = Self::set_validity(col.inner(), &validity.slice(0, 1))?;
            Ok(ConstColumn::new(inner, col.len()).arc())
        } else if column.is_nullable() {
            let col: &NullableColumn = Series::check_get(column)?;
            // It's possible validity is longer than col.
            let diff_len = validity.len() - col.ensure_validity().len();
            let mut new_validity = MutableBitmap::with_capacity(validity.len());
            for (b1, b2) in validity.iter().zip(col.ensure_validity().iter()) {
                new_validity.push(b1 & b2);
            }
            new_validity.extend_constant(diff_len, false);
            let col = NullableColumn::wrap_inner(col.inner().clone(), Some(new_validity.into()));
            Ok(col)
        } else {
            let col = NullableColumn::wrap_inner(column.clone(), Some(validity.clone()));
            Ok(col)
        }
    }

    #[inline]
    fn probe_key<Key: HashTableKeyable>(
        hash_table: &HashMap<Key, Vec<RowPtr>>,
        key: Key,
        valids: &Option<Bitmap>,
        i: usize,
    ) -> Option<*mut KeyValueEntity<Key, Vec<RowPtr>>> {
        if valids.as_ref().map_or(true, |v| v.get_bit(i)) {
            return hash_table.find_key(&key);
        }
        None
    }
}
