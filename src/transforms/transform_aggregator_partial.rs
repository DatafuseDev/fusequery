// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use std::sync::Arc;

use async_trait::async_trait;
use futures::stream::StreamExt;

use crate::contexts::FuseQueryContextRef;
use crate::datablocks::DataBlock;
use crate::datastreams::{DataBlockStream, SendableDataBlockStream};
use crate::datavalues::{DataSchemaRef, DataValue, StringArray};
use crate::error::FuseQueryResult;
use crate::functions::IFunction;
use crate::planners::ExpressionPlan;
use crate::processors::{EmptyProcessor, IProcessor};

pub struct AggregatorPartialTransform {
    funcs: Vec<Box<dyn IFunction>>,
    schema: DataSchemaRef,
    input: Arc<dyn IProcessor>,
}

impl AggregatorPartialTransform {
    pub fn try_create(
        ctx: FuseQueryContextRef,
        schema: DataSchemaRef,
        exprs: Vec<ExpressionPlan>,
    ) -> FuseQueryResult<Self> {
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            funcs.push(expr.to_function(ctx.clone())?);
        }

        Ok(AggregatorPartialTransform {
            funcs,
            schema,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for AggregatorPartialTransform {
    fn name(&self) -> &str {
        "AggregatePartialTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> FuseQueryResult<()> {
        self.input = input;
        Ok(())
    }

    async fn execute(&self) -> FuseQueryResult<SendableDataBlockStream> {
        let mut funcs = self.funcs.clone();
        let mut stream = self.input.execute().await?;
        while let Some(block) = stream.next().await {
            let block = block?;

            for func in funcs.iter_mut() {
                func.accumulate(&block)?;
            }
        }

        let mut acc_results = Vec::with_capacity(funcs.len());
        for func in &funcs {
            let states = DataValue::Struct(func.accumulate_result()?);
            let serialized = serde_json::to_string(&states)?;
            acc_results.push(serialized);
        }

        let partial_results = acc_results
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<&str>>();
        let block = DataBlock::create(
            self.schema.clone(),
            vec![Arc::new(StringArray::from(partial_results))],
        );
        Ok(Box::pin(DataBlockStream::create(
            self.schema.clone(),
            None,
            vec![block],
        )))
    }
}
