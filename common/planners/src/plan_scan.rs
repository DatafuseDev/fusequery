// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;

use crate::ExpressionAction;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ScanPlan {
    /// The name of the schema
    pub schema_name: String,
    /// The schema of the source data
    pub table_schema: DataSchemaRef,
    pub table_args: Option<ExpressionAction>,
    /// Optional column indices to use as a projection
    pub projection: Option<Vec<usize>>,
    /// The schema description of the output
    pub projected_schema: DataSchemaRef,
    /// Optional filter expression plan
    pub filters: Vec<ExpressionAction>,
    /// Optional limit to skip read
    pub limit: Option<usize>,
}

impl ScanPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.projected_schema.clone()
    }

    pub fn empty() -> Self {
        Self {
            schema_name: "".to_string(),
            table_schema: Arc::new(DataSchema::empty()),
            table_args: None,
            projection: None,
            projected_schema: Arc::new(DataSchema::empty()),
            filters: vec![],
            limit: None,
        }
    }
}
