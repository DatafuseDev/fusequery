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
use std::collections::HashSet;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::VirtualColumnInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;
use opendal::Operator;
use storages_common_table_meta::table::TableCompression;

use crate::io::BlockReader;

#[derive(Clone)]
pub struct VirtualColumnReader {
    pub(super) ctx: Arc<dyn TableContext>,
    pub(super) dal: Operator,
    pub(super) source_schema: TableSchemaRef,
    pub prewhere_schema: Option<TableSchemaRef>,
    pub output_schema: TableSchemaRef,
    pub(super) reader: Arc<BlockReader>,
    pub(super) compression: TableCompression,
    pub virtual_column_infos: Vec<VirtualColumnInfo>,
    // Record the number of virtual columns for each source column,
    // if all the virtual columns have been generated,
    // and the source columns do not need for output and prewhere,
    // we ignore to read the source columns.
    pub(super) virtual_src_cnts: HashMap<String, usize>,
}

impl VirtualColumnReader {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        source_schema: TableSchemaRef,
        plan: &DataSourcePlan,
        virtual_column_infos: Vec<VirtualColumnInfo>,
        compression: TableCompression,
    ) -> Result<Self> {
        let prewhere_schema =
            if let Some(v) = PushDownInfo::prewhere_of_push_downs(&plan.push_downs) {
                let prewhere_schema = v
                    .prewhere_columns
                    .project_schema(plan.source_info.schema().as_ref());
                Some(Arc::new(prewhere_schema))
            } else {
                None
            };
        let output_schema = plan.schema();

        let mut virtual_src_cnts = HashMap::new();
        for virtual_column in &virtual_column_infos {
            if let Some(cnt) = virtual_src_cnts.get_mut(&virtual_column.source_name) {
                *cnt += 1;
            } else {
                virtual_src_cnts.insert(virtual_column.source_name.clone(), 1);
            }
        }

        // Each virtual columns source may have different schemas,
        // read the real schema from the file's meta
        let reader = BlockReader::create(
            dal.clone(),
            TableSchemaRefExt::create(vec![]),
            Projection::Columns(vec![]),
            ctx.clone(),
            false,
        )?;

        Ok(Self {
            ctx,
            dal,
            source_schema,
            prewhere_schema,
            output_schema,
            reader,
            compression,
            virtual_column_infos,
            virtual_src_cnts,
        })
    }

    pub(super) fn generate_ignore_column_ids(
        &self,
        virtual_src_cnts: HashMap<String, usize>,
    ) -> Option<HashSet<ColumnId>> {
        let mut ignore_column_ids = HashSet::new();
        for (src_name, cnt) in virtual_src_cnts.iter() {
            // cnt is zero means all the virtual columns have be generated,
            // if the source columns is not need for output and prewhere,
            // we can ignore to read the column.
            if *cnt == 0 && self.output_schema.index_of(src_name).is_err() {
                if let Some(prewhere_schema) = &self.prewhere_schema {
                    if prewhere_schema.index_of(src_name).is_ok() {
                        continue;
                    }
                }
                let field = self.source_schema.field_with_name(src_name).ok()?;
                ignore_column_ids.insert(field.column_id());
            }
        }
        if !ignore_column_ids.is_empty() {
            Some(ignore_column_ids)
        } else {
            None
        }
    }
}
