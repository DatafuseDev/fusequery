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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::optimizer::dynamic_sample::filter_selectivity_sample::filter_selectivity_sample;
use crate::optimizer::dynamic_sample::join_selectivity_sample::join_selectivity_sample;
use crate::optimizer::QuerySampleExecutor;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::StatInfo;
use crate::plans::Operator;
use crate::plans::RelOperator;
use crate::MetadataRef;

#[async_recursion::async_recursion(#[recursive::recursive])]
pub async fn dynamic_sample(
    ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    s_expr: &SExpr,
    sample_executor: Arc<dyn QuerySampleExecutor>,
) -> Result<Arc<StatInfo>> {
    match s_expr.plan() {
        RelOperator::Filter(_) => {
            filter_selectivity_sample(ctx, metadata, s_expr, sample_executor).await
        }
        RelOperator::Join(_) => {
            join_selectivity_sample(ctx, metadata, s_expr, sample_executor).await
        }
        RelOperator::Scan(_) => s_expr.plan().derive_stats(&RelExpr::with_s_expr(s_expr)),
        _ => Err(ErrorCode::Unimplemented(format!(
            "derive_cardinality_by_sample for {:?} is not supported yet",
            s_expr.plan()
        ))),
    }
}
