// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;
use pretty_assertions::assert_eq;

use crate::interpreters::*;
use crate::sql::*;

#[tokio::test]
async fn interpreter_describe_table_test() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;

    // Create table.
    {
        if let PlanNode::CreateTable(plan) = PlanParser::create(ctx.clone())
            .build_from_sql("create table default.a(a bigint, b int, c varchar(255), d smallint, e Date ) Engine = Null")?
        {
            let executor = CreateTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            let _ = executor.execute().await?;
        }
    }

    // describe table.
    {
        if let PlanNode::DescribeTable(plan) =
            PlanParser::create(ctx.clone()).build_from_sql("describe a")?
        {
            let executor = DescribeTableInterpreter::try_create(ctx.clone(), plan.clone())?;
            assert_eq!(executor.name(), "DescribeTableInterpreter");

            let stream = executor.execute().await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            let expected = vec![
                "+-------+--------+------+",
                "| Field | Type   | Null |",
                "+-------+--------+------+",
                "| a     | Int64  | NO   |",
                "| b     | Int32  | NO   |",
                "| c     | Utf8   | NO   |",
                "| d     | Int16  | NO   |",
                "| e     | Date32 | NO   |",
                "+-------+--------+------+",
            ];
            common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());
        } else {
            assert!(false)
        }
    }

    Ok(())
}
