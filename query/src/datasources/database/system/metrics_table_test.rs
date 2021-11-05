// Copyright 2020 Datafuse Labs.
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

use common_base::tokio;
use common_datablocks::pretty_format_blocks;
use common_exception::Result;
use common_metrics::init_default_metrics_recorder;
use futures::TryStreamExt;

use crate::catalogs::Table;
use crate::catalogs::ToReadDataSourcePlan;
use crate::datasources::database::system::MetricsTable;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_metrics_table() -> Result<()> {
    init_default_metrics_recorder();
    let ctx = crate::tests::try_create_context()?;
    let table: Arc<dyn Table> = Arc::new(MetricsTable::create(1));
    let io_ctx = ctx.get_cluster_table_io_context()?;
    let io_ctx = Arc::new(io_ctx);
    let source_plan = table.read_plan(io_ctx.clone(), None)?;
    metrics::counter!("test.test_metrics_table_count", 1);
    metrics::histogram!("test.test_metrics_table_histogram", 1.0);

    let stream = table.read(io_ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);
    assert!(block.num_rows() >= 1);

    let output = pretty_format_blocks(result.as_slice())?;
    assert!(output.contains("test_test_metrics_table_count"));
    assert!(output.contains("test_test_metrics_table_histogram"));
    assert!(output.contains("[{\"quantile\":0.0,\"count\":1.0},{\"quantile\":0.5,\"count\":1.0},{\"quantile\":0.9,\"count\":1.0},{\"quantile\":0.95,\"count\":1.0},{\"quantile\":0.99,\"count\":1.0},{\"quantile\":0.999,\"count\":1.0},{\"quantile\":1.0,\"count\":1.0}]"));

    Ok(())
}
