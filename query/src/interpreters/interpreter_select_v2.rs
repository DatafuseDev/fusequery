// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;

use crate::interpreters::stream::ProcessorExecutorStream;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::new::executor::PipelineExecutor;
use crate::pipelines::new::executor::PipelinePullingExecutor;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::sql::exec::PipelineBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::BindContext;
use crate::sql::MetadataRef;

/// Interpret SQL query with new SQL planner
pub struct SelectInterpreterV2 {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    bind_context: BindContext,
    metadata: MetadataRef,
}

impl SelectInterpreterV2 {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        bind_context: BindContext,
        s_expr: SExpr,
        metadata: MetadataRef,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(SelectInterpreterV2 {
            ctx,
            s_expr,
            bind_context,
            metadata,
        }))
    }
}

#[async_trait::async_trait]
impl Interpreter for SelectInterpreterV2 {
    fn name(&self) -> &str {
        "SelectInterpreterV2"
    }

    #[tracing::instrument(level = "debug", name = "select_interpreter_v2_execute", skip(self, _input_stream), fields(ctx.id = self.ctx.get_id().as_str()))]
    async fn execute(
        &self,
        _input_stream: Option<SendableDataBlockStream>,
    ) -> Result<SendableDataBlockStream> {
        let pb = PipelineBuilder::new(
            self.ctx.clone(),
            self.bind_context.result_columns(),
            self.metadata.clone(),
            self.s_expr.clone(),
        );

        if let Some(handle) = self.ctx.get_http_query() {
            return handle.execute(self.ctx.clone(), pb).await;
        }
        let (root_pipeline, pipelines, _) = pb.spawn()?;
        let async_runtime = self.ctx.get_storage_runtime();

        // Spawn sub-pipelines
        for pipeline in pipelines {
            let executor = PipelineExecutor::create(async_runtime.clone(), pipeline)?;
            executor.execute()?;
        }

        // Spawn root pipeline
        let executor = PipelinePullingExecutor::try_create(async_runtime, root_pipeline)?;
        let (handler, stream) = ProcessorExecutorStream::create(executor)?;
        self.ctx.add_source_abort_handle(handler);
        Ok(Box::pin(Box::pin(stream)))
    }

    /// This method will create a new pipeline
    /// The QueryPipelineBuilder will use the optimized plan to generate a NewPipeline
    async fn create_new_pipeline(&self) -> Result<NewPipeline> {
        let builder = PipelineBuilder::new(
            self.ctx.clone(),
            self.bind_context.result_columns(),
            self.metadata.clone(),
            self.s_expr.clone(),
        );
        let new_pipeline = builder.spawn()?.0;
        Ok(new_pipeline)
    }

    async fn start(&self) -> Result<()> {
        Ok(())
    }

    async fn finish(&self) -> Result<()> {
        Ok(())
    }
}
