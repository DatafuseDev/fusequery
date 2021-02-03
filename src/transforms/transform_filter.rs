// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use async_trait::async_trait;
use std::sync::Arc;

use arrow::compute::filter_record_batch;

use crate::datablocks::DataBlock;
use crate::datastreams::{ExpressionStream, SendableDataBlockStream};
use crate::datavalues::{BooleanArray, DataSchema, DataSchemaRef};
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::IFunction;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct FilterTransform {
    func: Box<dyn IFunction>,
    input: Arc<dyn IProcessor>,
}

impl FilterTransform {
    pub fn try_create(predicate: ExpressionPlan) -> FuseQueryResult<Self> {
        let func = predicate.to_function()?;
        if func.is_aggregator() {
            return Err(FuseQueryError::Internal(format!(
                "Aggregate function {:?} is found in WHERE in query",
                predicate
            )));
        }

        Ok(FilterTransform {
            func,
            input: Arc::new(EmptyProcessor::create()),
        })
    }

    pub fn expression_executor(
        _schema: &DataSchemaRef,
        block: DataBlock,
        funcs: Vec<Box<dyn IFunction>>,
    ) -> FuseQueryResult<DataBlock> {
        let func = funcs[0].clone();
        let result = func.eval(&block)?.to_array(block.num_rows())?;
        let filter_result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                FuseQueryError::Internal("cannot downcast to boolean array".to_string())
            })?;
        Ok(DataBlock::try_from_arrow_batch(&filter_record_batch(
            &block.to_arrow_batch()?,
            filter_result,
        )?)?)
    }
}

#[async_trait]
impl IProcessor for FilterTransform {
    fn name(&self) -> &str {
        "FilterTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        Ok(Box::pin(ExpressionStream::try_create(
            self.input.execute().await?,
            Arc::new(DataSchema::empty()),
            vec![self.func.clone()],
            FilterTransform::expression_executor,
        )?))
    }
}
