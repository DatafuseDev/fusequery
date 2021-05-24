// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use common_datablocks::SortColumnDescription;
use common_datavalues::DataSchemaRef;
use common_exception::ErrorCodes;
use common_exception::Result;
use common_planners::ExpressionAction;
use common_streams::SendableDataBlockStream;
use common_streams::SortStream;

use crate::pipelines::processors::EmptyProcessor;
use crate::pipelines::processors::IProcessor;

pub struct SortPartialTransform {
    schema: DataSchemaRef,
    exprs: Vec<ExpressionAction>,
    limit: Option<usize>,
    input: Arc<dyn IProcessor>,
}

impl SortPartialTransform {
    pub fn try_create(
        schema: DataSchemaRef,
        exprs: Vec<ExpressionAction>,
        limit: Option<usize>,
    ) -> Result<Self> {
        Ok(SortPartialTransform {
            schema,
            exprs,
            limit,
            input: Arc::new(EmptyProcessor::create()),
        })
    }
}

#[async_trait]
impl IProcessor for SortPartialTransform {
    fn name(&self) -> &str {
        "SortPartialTransform"
    }

    fn connect_to(&mut self, input: Arc<dyn IProcessor>) -> Result<()> {
        self.input = input;
        Ok(())
    }

    fn inputs(&self) -> Vec<Arc<dyn IProcessor>> {
        vec![self.input.clone()]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(SortStream::try_create(
            self.input.execute().await?,
            get_sort_descriptions(&self.schema, &self.exprs)?,
            self.limit,
        )?))
    }
}

pub fn get_sort_descriptions(
    schema: &DataSchemaRef,
    exprs: &[ExpressionAction],
) -> Result<Vec<SortColumnDescription>> {
    let mut sort_columns_descriptions = vec![];
    for x in exprs {
        match *x {
            ExpressionAction::Sort {
                ref expr,
                asc,
                nulls_first,
            } => {
                let column_name = expr.to_data_field(schema)?.name().clone();
                sort_columns_descriptions.push(SortColumnDescription {
                    column_name,
                    asc,
                    nulls_first,
                });
            }
            _ => {
                return Result::Err(ErrorCodes::BadTransformType(format!(
                    "Sort expression must be ExpressionPlan::Sort, but got: {:?}",
                    x
                )));
            }
        }
    }
    Ok(sort_columns_descriptions)
}
