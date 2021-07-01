// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use common_datablocks::*;
use common_datavalues::prelude::*;
use common_progress::*;
use common_runtime::tokio;
use futures::TryStreamExt;

use crate::*;

#[tokio::test]
async fn test_progress_stream() -> anyhow::Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int64, false)]);

    let block =
        DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1i64, 2, 3]).into()]);

    let input = DataBlockStream::create(Arc::new(DataSchema::empty()), None, vec![
        block.clone(),
        block.clone(),
        block,
    ]);

    let mut all_rows = 0;
    let progress = Box::new(move |value: &ProgressValues| {
        all_rows += value.read_rows;
        println!("{}", all_rows);
    });

    let stream = ProgressStream::try_create(Box::pin(input), progress)?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+---+", "| a |", "+---+", "| 1 |", "| 1 |", "| 1 |", "| 2 |", "| 2 |", "| 2 |", "| 3 |",
        "| 3 |", "| 3 |", "+---+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}
