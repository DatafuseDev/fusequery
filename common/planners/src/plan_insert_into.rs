// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;
use std::sync::Mutex;

use common_datablocks::DataBlock;
use common_datavalues::DataSchemaRef;

/// please do not keep this, this code is just for test purpose
type BlockStream =
    std::pin::Pin<Box<dyn futures::stream::Stream<Item = DataBlock> + Sync + Send + 'static>>;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct InsertIntoPlan {
    pub db_name: String,
    pub tbl_name: String,
    pub schema: DataSchemaRef,

    #[serde(skip, default = "InsertIntoPlan::empty_stream")]
    pub input_stream: Arc<Mutex<Option<BlockStream>>>
}

impl InsertIntoPlan {
    pub fn empty_stream() -> Arc<Mutex<Option<BlockStream>>> {
        Default::default()
    }
}

impl InsertIntoPlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
