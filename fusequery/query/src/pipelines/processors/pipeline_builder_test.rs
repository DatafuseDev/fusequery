// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_distributed_pipeline_build() -> anyhow::Result<()> {
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::sql::*;

    let ctx = crate::tests::try_create_context_with_nodes(3).await?;
    let cpus = ctx.get_max_threads()?;

    // For more partitions generation.
    let ctx_more_cpu = crate::tests::try_create_context()?;
    ctx_more_cpu.set_max_threads(cpus * 40)?;

    let plan = PlanParser::create(ctx_more_cpu.clone()).build_from_sql(
        "select sum(number+1)+2 as sumx from numbers_mt(80000) where (number+1)=4 limit 1"
    )?;
    let pipeline = PipelineBuilder::create(ctx, plan).build()?;
    let expect = "LimitTransform × 1 processor\
    \n  ProjectionTransform × 1 processor\
    \n    ExpressionTransform × 1 processor\
    \n      AggregatorFinalTransform × 1 processor\
    \n        Merge (RemoteTransform × 3 processors) to (AggregatorFinalTransform × 1)\
    \n          RemoteTransform × 3 processor(s): AggregatorPartialTransform × 8 processors -> ExpressionTransform × 8 processors ->   FilterTransform × 8 processors ->     SourceTransform × 8 processors";
    let actual = format!("{:?}", pipeline);
    assert_eq!(expect, actual);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_local_pipeline_builds() -> anyhow::Result<()> {
    use futures::TryStreamExt;
    use pretty_assertions::assert_eq;

    use crate::pipelines::processors::*;
    use crate::sql::*;

    struct Test {
        name: &'static str,
        query: &'static str,
        plan: &'static str,
        pipeline: &'static str,
        block: Vec<&'static str>
    }

    let tests = vec![
        Test {
            name: "select-alias-pass",
            query: "select number as c1, number as c2 from numbers_mt(10) order by c1 desc",

            plan: "\
            Projection: number as c1:UInt64, number as c2:UInt64\
            \n  Sort: number:UInt64\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]",

            pipeline: "\
            ProjectionTransform × 1 processor\
            \n  SortMergeTransform × 1 processor\
            \n    Merge (SortMergeTransform × 8 processors) to (SortMergeTransform × 1)\
            \n      SortMergeTransform × 8 processors\
            \n        SortPartialTransform × 8 processors\
            \n          SourceTransform × 8 processors",


            block: vec![
                "+----+----+",
                "| c1 | c2 |",
                "+----+----+",
                "| 9  | 9  |",
                "| 8  | 8  |",
                "| 7  | 7  |",
                "| 6  | 6  |",
                "| 5  | 5  |",
                "| 4  | 4  |",
                "| 3  | 3  |",
                "| 2  | 2  |",
                "| 1  | 1  |",
                "| 0  | 0  |",
                "+----+----+",
            ]
        },
        Test {
            name: "select-order-by-alias-pass",
            query: "select number as c1, number as c2 from numbers_mt(10) order by c1 desc, c2 asc",

            plan: "\
            Projection: number as c1:UInt64, number as c2:UInt64\
            \n  Sort: number:UInt64, number:UInt64\
            \n    ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]",


            pipeline: "\
            ProjectionTransform × 1 processor\
            \n  SortMergeTransform × 1 processor\
            \n    Merge (SortMergeTransform × 8 processors) to (SortMergeTransform × 1)\
            \n      SortMergeTransform × 8 processors\
            \n        SortPartialTransform × 8 processors\
            \n          SourceTransform × 8 processors",

            block: vec![
                "+----+----+",
                "| c1 | c2 |",
                "+----+----+",
                "| 9  | 9  |",
                "| 8  | 8  |",
                "| 7  | 7  |",
                "| 6  | 6  |",
                "| 5  | 5  |",
                "| 4  | 4  |",
                "| 3  | 3  |",
                "| 2  | 2  |",
                "| 1  | 1  |",
                "| 0  | 0  |",
                "+----+----+",
            ]
        },
        Test {
            name: "select-order-by-alias-expression-pass",
            query:
                "select number as c1, (number + 1) as c2 from numbers_mt(10) order by c1 desc, c2 asc",

            plan: "\
            Projection: number as c1:UInt64, (number + 1) as c2:UInt64\
            \n  Sort: number:UInt64, (number + 1):UInt64\
            \n    Expression: number:UInt64, (number + 1):UInt64 (Before OrderBy)\
            \n      ReadDataSource: scan partitions: [8], scan schema: [number:UInt64], statistics: [read_rows: 10, read_bytes: 80]",

            pipeline: "\
            ProjectionTransform × 1 processor\
            \n  SortMergeTransform × 1 processor\
            \n    Merge (SortMergeTransform × 8 processors) to (SortMergeTransform × 1)\
            \n      SortMergeTransform × 8 processors\
            \n        SortPartialTransform × 8 processors\
            \n          ExpressionTransform × 8 processors\
            \n            SourceTransform × 8 processors",

            block: vec![
                "+----+----+",
                "| c1 | c2 |",
                "+----+----+",
                "| 9  | 10 |",
                "| 8  | 9  |",
                "| 7  | 8  |",
                "| 6  | 7  |",
                "| 5  | 6  |",
                "| 4  | 5  |",
                "| 3  | 4  |",
                "| 2  | 3  |",
                "| 1  | 2  |",
                "| 0  | 1  |",
                "+----+----+",
            ]
        },
    ];

    let ctx = crate::tests::try_create_context()?;
    for test in tests {
        // Plan build check.
        let plan = PlanParser::create(ctx.clone()).build_from_sql(test.query)?;
        let actual_plan = format!("{:?}", plan);
        assert_eq!(test.plan, actual_plan, "{:#?}", test.name);

        // Pipeline build check.
        let mut pipeline = PipelineBuilder::create(ctx.clone(), plan).build()?;
        let actual_pipeline = format!("{:?}", pipeline);
        assert_eq!(test.pipeline, actual_pipeline, "{:#?}", test.name);

        // Result check.
        let stream = pipeline.execute().await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        common_datablocks::assert_blocks_eq_with_name(test.name, test.block, result.as_slice());
    }
    Ok(())
}
