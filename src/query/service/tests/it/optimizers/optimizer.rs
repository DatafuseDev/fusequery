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

use std::any::Any;
use std::sync::Arc;

use common_planners::*;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
struct OptimizerTestPartInfo {}

#[typetag::serde(name = "optimizer_test")]
impl PartInfo for OptimizerTestPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<OptimizerTestPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }
}

impl OptimizerTestPartInfo {
    pub fn create() -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(OptimizerTestPartInfo {}))
    }
}

pub fn generate_partitions(workers: u64, total: u64) -> Partitions {
    let part_size = total / workers;
    // let part_remain = total % workers;

    let mut partitions = Vec::with_capacity(workers as usize);
    if part_size == 0 {
        partitions.push(OptimizerTestPartInfo::create())
    } else {
        for _part in 0..workers {
            // let part_begin = part * part_size;
            // let mut part_end = (part + 1) * part_size;
            // if part == (workers - 1) && part_remain > 0 {
            //     part_end += part_remain;
            // }
            partitions.push(OptimizerTestPartInfo::create())
        }
    }
    partitions
}

use common_base::base::tokio;
use common_exception::Result;
use databend_query::optimizers::Optimizers;
use databend_query::sql::PlanParser;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_literal_false_filter() -> Result<()> {
    let query = "select * from numbers_mt(10) where 1 + 2 = 2";
    let (_guard, ctx) = crate::tests::create_query_context().await?;

    let plan = PlanParser::parse(ctx.clone(), query).await?;
    let mut optimizer = Optimizers::without_scatters(ctx);
    let optimized = optimizer.optimize(&plan)?;
    let actual = format!("{:?}", optimized);

    let expect = "\
        Projection: number:UInt64\
        \n  Filter: false\
        \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0], filters: [((1 + 2) = 2)]]";

    assert_eq!(actual, expect);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_skip_read_data_source() -> Result<()> {
    struct Test {
        name: &'static str,
        query: &'static str,
        expect: &'static str,
    }

    let tests: Vec<Test> = vec![
        Test {
            name: "Filter with 'where 1 + 2 = 2' should skip the scan",
            query: "select * from numbers_mt(10) where 1 + 2 = 2",
            expect: "\
                Projection: number:UInt64\
                \n  Filter: false\
                \n    ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0], filters: [((1 + 2) = 2)]]",
        },
        Test {
            name: "Limit with zero should skip the scan",
            query: "select * from numbers_mt(10) where true limit 0",
            expect: "\
                Limit: 0\
                \n  Projection: number:UInt64\n    Filter: true\n      ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0], filters: [true], limit: 0]",
        },
        Test {
            name: "Having with 'having 1+1=3' should skip the scan",
            query: "select avg(number) from numbers_mt(100) group by number%10 having 1+1=3",
            expect: "\
                Projection: avg(number):Float64\
                \n  Having: false\
                \n    AggregatorFinal: groupBy=[[(number % 10)]], aggr=[[avg(number)]]\
                \n      AggregatorPartial: groupBy=[[(number % 10)]], aggr=[[avg(number)]]\
                \n        Expression: (number % 10):UInt8, number:UInt64 (Before GroupBy)\
                \n          ReadDataSource: scan schema: [number:UInt64], statistics: [read_rows: 0, read_bytes: 0, partitions_scanned: 0, partitions_total: 0], push_downs: [projections: [0]]",
        },
    ];

    let (_guard, ctx) = crate::tests::create_query_context().await?;
    for test in tests {
        let plan = PlanParser::parse(ctx.clone(), test.query).await?;
        let mut optimizer = Optimizers::without_scatters(ctx.clone());

        let optimized_plan = optimizer.optimize(&plan)?;
        let actual = format!("{:?}", optimized_plan);
        assert_eq!(test.expect, actual, "{:#?}", test.name);
    }
    Ok(())
}
