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

use std::alloc::Layout;
use std::fmt;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_io::prelude::BinaryWrite;

use crate::aggregates::AggrState;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

#[derive(Clone)]
pub struct AggregateNullUnaryAdaptor<const NULLABLE_RESULT: bool> {
    nested: AggregateFunctionRef,
    size_of_data: usize,
}

impl<const NULLABLE_RESULT: bool> AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    pub fn create(nested: AggregateFunctionRef) -> AggregateFunctionRef {
        let size_of_data = if NULLABLE_RESULT {
            let layout = nested.state_layout();
            layout.size()
        } else {
            0
        };
        Arc::new(Self {
            nested,
            size_of_data,
        })
    }

    #[inline]
    pub fn set_flag(&self, place: AggrState, flag: u8) {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c = flag;
        }
    }

    #[inline]
    pub fn init_flag(&self, place: AggrState) {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c = 0;
        }
    }

    #[inline]
    pub fn get_flag(&self, place: AggrState) -> u8 {
        if NULLABLE_RESULT {
            let c = place.next(self.size_of_data).get::<u8>();
            *c
        } else {
            1
        }
    }
}

impl<const NULLABLE_RESULT: bool> AggregateFunction for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn name(&self) -> &str {
        "AggregateNullUnaryAdaptor"
    }

    fn return_type(&self) -> Result<DataType> {
        let nested = self.nested.return_type()?;
        match NULLABLE_RESULT {
            true => Ok(nested.wrap_nullable()),
            false => Ok(nested),
        }
    }

    #[inline]
    fn init_state(&self, place: AggrState) {
        self.init_flag(place);
        self.nested.init_state(place);
    }

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.nested.serialize_size_per_row().map(|row| row + 1)
    }

    #[inline]
    fn state_layout(&self) -> Layout {
        let layout = self.nested.state_layout();
        let add = if NULLABLE_RESULT { layout.align() } else { 0 };
        Layout::from_size_align(layout.size() + add, layout.align()).unwrap()
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, validity.cloned());
        let not_null_column = &[col.remove_nullable()];
        let not_null_column = not_null_column.into();
        let validity = Bitmap::map_all_sets_to_none(validity);

        self.nested
            .accumulate(place, not_null_column, validity.as_ref(), input_rows)?;

        if validity
            .as_ref()
            .map(|c| c.null_count() != input_rows)
            .unwrap_or(true)
        {
            self.set_flag(place, 1);
        }
        Ok(())
    }

    #[inline]
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: InputColumns,
        input_rows: usize,
    ) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, None);
        let not_null_columns = &[col.remove_nullable()];
        let not_null_columns = not_null_columns.into();

        match validity {
            Some(v) if v.null_count() > 0 => {
                // all nulls
                if v.null_count() == v.len() {
                    return Ok(());
                }

                for (valid, (row, place)) in v.iter().zip(places.iter().enumerate()) {
                    if valid {
                        let place = AggrState {
                            addr: *place,
                            offset,
                        };
                        self.set_flag(place, 1);
                        self.nested.accumulate_row(place, not_null_columns, row)?;
                    }
                }
            }
            _ => {
                self.nested
                    .accumulate_keys(places, offset, not_null_columns, input_rows)?;
                places.iter().for_each(|place| {
                    self.set_flag(
                        AggrState {
                            addr: *place,
                            offset,
                        },
                        1,
                    )
                });
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let col = &columns[0];
        let validity = column_merge_validity(col, None);
        let not_null_columns = &[col.remove_nullable()];
        let not_null_columns = not_null_columns.into();

        match validity {
            Some(v) if v.null_count() > 0 => {
                // all nulls
                if v.null_count() == v.len() {
                    return Ok(());
                }

                if unsafe { v.get_bit_unchecked(row) } {
                    self.set_flag(place, 1);
                    self.nested.accumulate_row(place, not_null_columns, row)?;
                }
            }
            _ => {
                self.nested.accumulate_row(place, not_null_columns, row)?;
                self.set_flag(place, 1);
            }
        }

        Ok(())
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        self.nested.serialize(place, writer)?;
        if NULLABLE_RESULT {
            let flag = self.get_flag(place);
            writer.write_scalar(&flag)?;
        }
        Ok(())
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        if self.get_flag(place) == 0 {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }

        if NULLABLE_RESULT {
            let flag = reader[reader.len() - 1];
            if flag == 1 {
                self.set_flag(place, 1);
                self.nested.merge(place, &mut &reader[..reader.len() - 1])?;
            }
        } else {
            self.nested.merge(place, reader)?;
        }

        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        if self.get_flag(place) == 0 {
            // initial the state to remove the dirty stats
            self.init_state(place);
        }

        if self.get_flag(rhs) == 1 {
            self.set_flag(place, 1);
            self.nested.merge_states(place, rhs)?;
        }

        Ok(())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        if NULLABLE_RESULT {
            if self.get_flag(place) == 1 {
                match builder {
                    ColumnBuilder::Nullable(ref mut inner) => {
                        self.nested.merge_result(place, &mut inner.builder)?;
                        inner.validity.push(true);
                    }
                    _ => unreachable!(),
                }
            } else {
                builder.push_default();
            }
            Ok(())
        } else {
            self.nested.merge_result(place, builder)
        }
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: AggrState) {
        self.nested.drop_state(place)
    }

    fn convert_const_to_full(&self) -> bool {
        self.nested.convert_const_to_full()
    }

    fn get_if_condition(&self, columns: InputColumns) -> Option<Bitmap> {
        self.nested.get_if_condition(columns)
    }
}

impl<const NULLABLE_RESULT: bool> fmt::Display for AggregateNullUnaryAdaptor<NULLABLE_RESULT> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AggregateNullUnaryAdaptor")
    }
}
