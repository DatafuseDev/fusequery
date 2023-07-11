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
use std::time::Instant;

use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::DataSchemaRef;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;

use crate::metrics::metrics_inc_copy_append_data_cost_milliseconds;
use crate::metrics::metrics_inc_copy_append_data_counter;
use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOn;
use crate::sessions::QueryContext;

pub fn build_fill_missing_columns_pipeline(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
) -> Result<()> {
    let table_default_schema = &table.schema().remove_computed_fields();
    let table_computed_schema = &table.schema().remove_virtual_computed_fields();
    let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());
    let computed_schema: DataSchemaRef = Arc::new(table_computed_schema.into());

    // Fill missing default columns and resort the columns.
    if source_schema != default_schema {
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformResortAddOn::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                default_schema.clone(),
                table.clone(),
            )
        })?;
    }

    // Fill computed columns.
    if default_schema != computed_schema {
        pipeline.add_transform(|transform_input_port, transform_output_port| {
            TransformAddComputedColumns::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                default_schema.clone(),
                computed_schema.clone(),
            )
        })?;
    }

    Ok(())
}

pub fn build_append2table_with_commit_pipeline(
    ctx: Arc<QueryContext>,
    main_pipeline: &mut Pipeline,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    copied_files: Option<UpsertTableCopiedFileReq>,
    overwrite: bool,
    append_mode: AppendMode,
) -> Result<()> {
    build_fill_missing_columns_pipeline(ctx.clone(), main_pipeline, table.clone(), source_schema)?;

    table.append_data(ctx.clone(), main_pipeline, append_mode)?;

    table.commit_insertion(ctx, main_pipeline, copied_files, overwrite)?;

    Ok(())
}

pub fn build_append2table_without_commit_pipeline(
    ctx: Arc<QueryContext>,
    main_pipeline: &mut Pipeline,
    table: Arc<dyn Table>,
    source_schema: DataSchemaRef,
    append_mode: AppendMode,
) -> Result<()> {
    build_fill_missing_columns_pipeline(ctx.clone(), main_pipeline, table.clone(), source_schema)?;

    let start = Instant::now();
    table.append_data(ctx, main_pipeline, append_mode)?;

    // Perf
    {
        metrics_inc_copy_append_data_counter(1_u32);
        metrics_inc_copy_append_data_cost_milliseconds(start.elapsed().as_millis() as u32);
    }

    Ok(())
}
