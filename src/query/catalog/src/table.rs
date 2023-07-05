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
use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::NumberScalar;
use common_expression::BlockThresholds;
use common_expression::ColumnId;
use common_expression::FieldIndex;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableField;
use common_expression::TableSchema;
use common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use common_io::constants::DEFAULT_BLOCK_MIN_ROWS;
use common_meta_app::schema::DatabaseType;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::MetaId;
use common_pipeline_core::Pipeline;
use common_storage::StorageMetrics;

use crate::plan::DataSourceInfo;
use crate::plan::DataSourcePlan;
use crate::plan::PartStatistics;
use crate::plan::Partitions;
use crate::plan::PushDownInfo;
use crate::table::column_stats_provider_impls::DummyColumnStatisticsProvider;
use crate::table_args::TableArgs;
use crate::table_context::TableContext;

#[async_trait::async_trait]
pub trait Table: Sync + Send {
    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    fn engine(&self) -> &str {
        self.get_table_info().engine()
    }

    fn schema(&self) -> Arc<TableSchema> {
        self.get_table_info().schema()
    }

    fn options(&self) -> &BTreeMap<String, String> {
        self.get_table_info().options()
    }

    fn field_comments(&self) -> &Vec<String> {
        self.get_table_info().field_comments()
    }

    fn get_id(&self) -> MetaId {
        self.get_table_info().ident.table_id
    }

    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any;

    fn get_table_info(&self) -> &TableInfo;

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::TableSource(self.get_table_info().clone())
    }

    /// get_data_metrics will get data metrics from table.
    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        None
    }

    /// whether column prune(projection) can help in table read
    fn support_column_projection(&self) -> bool {
        false
    }

    /// whether table has the exact number of total rows
    fn has_exact_total_row_count(&self) -> bool {
        false
    }

    fn cluster_keys(&self, _ctx: Arc<dyn TableContext>) -> Vec<RemoteExpr<String>> {
        vec![]
    }

    /// Whether the table engine supports prewhere optimization.
    /// only Fuse Engine supports this.
    fn support_prewhere(&self) -> bool {
        false
    }

    fn support_index(&self) -> bool {
        false
    }

    /// Whether the table engine supports virtual columns optimization.
    fn support_virtual_columns(&self) -> bool {
        false
    }

    /// Whether the table engine supports virtual column `_row_id`.
    fn support_row_id_column(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn alter_table_cluster_keys(
        &self,
        ctx: Arc<dyn TableContext>,
        cluster_key: String,
    ) -> Result<()> {
        let (_, _) = (ctx, cluster_key);

        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Unsupported clustering keys for engine: {}",
            self.engine()
        )))
    }

    #[async_backtrace::framed]
    async fn drop_table_cluster_keys(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        let _ = ctx;

        Err(ErrorCode::UnsupportedEngineParams(format!(
            "Unsupported clustering keys for engine: {}",
            self.engine()
        )))
    }

    /// Gather partitions to be scanned according to the push_downs
    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let (_, _) = (ctx, push_downs);
        Err(ErrorCode::Unimplemented(format!(
            "read_partitions operation for table {} is not implemented. table engine : {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    /// Assembly the pipeline of reading data from storage, according to the plan
    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let (_, _, _) = (ctx, plan, pipeline);

        Err(ErrorCode::Unimplemented(format!(
            "read_data operation for table {} is not implemented. table engine : {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    /// Assembly the pipeline of appending data to storage
    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        append_mode: AppendMode,
    ) -> Result<()> {
        let (_, _, _) = (ctx, pipeline, append_mode);

        Err(ErrorCode::Unimplemented(format!(
            "append_data operation for table {} is not implemented. table engine : {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    #[async_backtrace::framed]
    async fn replace_into(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        on_conflict_fields: Vec<TableField>,
    ) -> Result<()> {
        let (_, _, _) = (ctx, pipeline, on_conflict_fields);

        Err(ErrorCode::Unimplemented(format!(
            "replace_into operation for table {} is not implemented. table engine : {}",
            self.name(),
            self.get_table_info().meta.engine
        )))
    }

    fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        copied_files: Option<UpsertTableCopiedFileReq>,
        overwrite: bool,
    ) -> Result<()> {
        let (_, _, _, _) = (ctx, copied_files, pipeline, overwrite);

        Ok(())
    }

    #[async_backtrace::framed]
    async fn truncate(&self, ctx: Arc<dyn TableContext>, purge: bool) -> Result<()> {
        let (_, _) = (ctx, purge);
        Ok(())
    }

    #[async_backtrace::framed]
    async fn purge(
        &self,
        ctx: Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
        limit: Option<usize>,
        keep_last_snapshot: bool,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        let (_, _, _, _, _) = (ctx, instant, limit, keep_last_snapshot, dry_run);

        Ok(None)
    }

    #[async_backtrace::framed]
    async fn analyze(&self, ctx: Arc<dyn TableContext>) -> Result<()> {
        let _ = ctx;

        Ok(())
    }

    fn table_statistics(&self) -> Result<Option<TableStatistics>> {
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn column_statistics_provider(&self) -> Result<Box<dyn ColumnStatisticsProvider>> {
        Ok(Box::new(DummyColumnStatisticsProvider))
    }

    #[async_backtrace::framed]
    async fn navigate_to(&self, instant: &NavigationPoint) -> Result<Arc<dyn Table>> {
        let _ = instant;

        Err(ErrorCode::Unimplemented(format!(
            "table {},  of engine type {}, does not support time travel",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[async_backtrace::framed]
    async fn delete(
        &self,
        ctx: Arc<dyn TableContext>,
        // - pass a ScalarExpr to Table::delete, and let the table's implementation of method `delete` do the
        //   inversion will be more concise, unfortunately, using type ScalarExpr introduces cyclic dependency.
        // - we can also pass a common_expression::Expr here, and later do the inversion at Expr level, but it is not recommended :(
        filter: Option<DeletionFilters>,
        col_indices: Vec<usize>,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let (_, _, _, _, _) = (ctx, filter, col_indices, pipeline, query_row_id_col);

        Err(ErrorCode::Unimplemented(format!(
            "table {}, engine type {}, does not support DELETE FROM",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    async fn update(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<FieldIndex>,
        update_list: Vec<(FieldIndex, RemoteExpr<String>)>,
        computed_list: BTreeMap<FieldIndex, RemoteExpr<String>>,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let (_, _, _, _, _, _, _) = (
            ctx,
            filter,
            col_indices,
            update_list,
            computed_list,
            query_row_id_col,
            pipeline,
        );

        Err(ErrorCode::Unimplemented(format!(
            "table {},  of engine type {}, does not support UPDATE",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    fn get_block_thresholds(&self) -> BlockThresholds {
        BlockThresholds {
            max_rows_per_block: DEFAULT_BLOCK_MAX_ROWS,
            min_rows_per_block: DEFAULT_BLOCK_MIN_ROWS,
            max_bytes_per_block: DEFAULT_BLOCK_BUFFER_SIZE,
        }
    }

    fn set_block_thresholds(&self, _thresholds: BlockThresholds) {
        unimplemented!()
    }

    // return false if the table does not need to be compacted.
    #[async_backtrace::framed]
    async fn compact(
        &self,
        ctx: Arc<dyn TableContext>,
        target: CompactTarget,
        limit: Option<usize>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let (_, _, _, _) = (ctx, target, limit, pipeline);

        Err(ErrorCode::Unimplemented(format!(
            "table {},  of engine type {}, does not support compact",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[async_backtrace::framed]
    async fn recluster(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        push_downs: Option<PushDownInfo>,
    ) -> Result<()> {
        let (_, _, _) = (ctx, pipeline, push_downs);

        Err(ErrorCode::Unimplemented(format!(
            "table {},  of engine type {}, does not support recluster",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    #[async_backtrace::framed]
    async fn revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        point: NavigationDescriptor,
    ) -> Result<()> {
        let (_, _) = (ctx, point);
        Err(ErrorCode::Unimplemented(format!(
            "table {},  of engine type {}, does not support revert",
            self.name(),
            self.get_table_info().engine(),
        )))
    }

    fn is_stage_table(&self) -> bool {
        false
    }

    fn result_can_be_cached(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
pub trait TableExt: Table {
    #[async_backtrace::framed]
    async fn refresh(&self, ctx: &dyn TableContext) -> Result<Arc<dyn Table>> {
        let table_info = self.get_table_info();
        let name = table_info.name.clone();
        let tid = table_info.ident.table_id;
        let catalog = ctx.get_catalog(table_info.catalog())?;
        let (ident, meta) = catalog.get_table_meta_by_id(tid).await?;
        let table_info: TableInfo = TableInfo {
            ident,
            desc: "".to_owned(),
            name,
            meta: meta.as_ref().clone(),
            tenant: "".to_owned(),
            db_type: DatabaseType::NormalDB,
        };
        catalog.get_table_by_info(&table_info)
    }
}

impl<T: ?Sized> TableExt for T where T: Table {}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NavigationPoint {
    SnapshotID(String),
    TimePoint(DateTime<Utc>),
}

#[derive(Debug, Copy, Clone)]
pub struct TableStatistics {
    pub num_rows: Option<u64>,
    pub data_size: Option<u64>,
    pub data_size_compressed: Option<u64>,
    pub index_size: Option<u64>,
    pub number_of_blocks: Option<u64>,
    pub number_of_segments: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ColumnStatistics {
    pub min: Scalar,
    pub max: Scalar,
    pub null_count: u64,
    pub number_of_distinct_values: u64,
}

pub enum CompactTarget {
    Blocks,
    Segments,
}

pub enum AppendMode {
    // From INSERT and RECUSTER operation
    Normal,
    // From COPY, Streaming load operation
    Copy,
}

pub trait ColumnStatisticsProvider {
    // returns the statistics of the given column, if any.
    // column_id is just the index of the column in table's schema
    fn column_statistics(&self, column_id: ColumnId) -> Option<ColumnStatistics>;

    // If the data type is int and max - min + 1 < ndv, then adjust ndv to max - min + 1.
    fn adjust_ndv_by_min_max(&self, mut ndv: u64, min: Scalar, max: Scalar) -> u64 {
        let mut range = match (min, max) {
            (Scalar::Number(min), Scalar::Number(max)) => match (min, max) {
                (NumberScalar::UInt8(min), NumberScalar::UInt8(max)) => (max - min) as u64,
                (NumberScalar::UInt16(min), NumberScalar::UInt16(max)) => (max - min) as u64,
                (NumberScalar::UInt32(min), NumberScalar::UInt32(max)) => (max - min) as u64,
                (NumberScalar::UInt64(min), NumberScalar::UInt64(max)) => max - min,
                (NumberScalar::Int8(min), NumberScalar::Int8(max)) => {
                    (max as i16 - min as i16) as u64
                }
                (NumberScalar::Int16(min), NumberScalar::Int16(max)) => {
                    (max as i32 - min as i32) as u64
                }
                (NumberScalar::Int32(min), NumberScalar::Int32(max)) => {
                    (max as i64 - min as i64) as u64
                }
                (NumberScalar::Int64(min), NumberScalar::Int64(max)) => {
                    (max as i128 - min as i128) as u64
                }
                _ => return ndv,
            },
            (Scalar::Timestamp(min), Scalar::Timestamp(max)) => (max as i128 - min as i128) as u64,
            (Scalar::Date(min), Scalar::Date(max)) => (max as i64 - min as i64) as u64,
            (Scalar::String(mut min), Scalar::String(mut max))
            | (Scalar::Variant(mut min), Scalar::Variant(mut max)) => {
                // There are 128 characters in ASCII code and 128^4 = 268435456 < 2^32 < 128^5.
                if min.is_empty() || max.is_empty() || min.len() > 4 || max.len() > 4 {
                    return ndv;
                }
                let mut min_value: u32 = 0;
                let mut max_value: u32 = 0;
                while min.len() != max.len() {
                    if min.len() < max.len() {
                        min.push(0);
                    } else {
                        max.push(0);
                    }
                }
                for idx in 0..min.len() {
                    min_value = min_value * 128 + min[idx] as u32;
                    max_value = max_value * 128 + max[idx] as u32;
                }
                (max_value - min_value) as u64
            }
            (Scalar::Boolean(min), Scalar::Boolean(max)) => {
                if min == max {
                    1
                } else {
                    2
                }
            }
            _ => return ndv,
        };
        range = range.saturating_add(1);
        if range < ndv {
            ndv = range;
        }
        ndv
    }
}

mod column_stats_provider_impls {
    use super::*;

    pub(super) struct DummyColumnStatisticsProvider;

    impl ColumnStatisticsProvider for DummyColumnStatisticsProvider {
        fn column_statistics(&self, _column_id: ColumnId) -> Option<ColumnStatistics> {
            None
        }
    }
}

pub struct NavigationDescriptor {
    pub database_name: String,
    pub point: NavigationPoint,
}

#[derive(Debug, Clone)]
pub struct DeletionFilters {
    // the filter expression for the deletion
    pub filter: RemoteExpr<String>,
    // just "not(filter)"
    pub inverted_filter: RemoteExpr<String>,
}
