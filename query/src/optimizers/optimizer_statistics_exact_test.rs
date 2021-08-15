// Copyright 2020 Datafuse Labs.
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

#[cfg(test)]
mod tests {
    use std::mem::size_of;
    use std::sync::Arc;

    use common_datavalues::*;
    use common_exception::Result;
    use common_planners::*;
    use pretty_assertions::assert_eq;

    use crate::optimizers::optimizer_test::*;
    use crate::optimizers::*;

    #[test]
    fn test_statistics_exact_optimizer() -> Result<()> {
        let ctx = crate::tests::try_create_context()?;

        let total = ctx.get_settings().get_max_block_size()? as u64;
        let statistics =
            Statistics::new_exact(total as usize, ((total) * size_of::<u64>() as u64) as usize);
        ctx.try_set_statistics(&statistics)?;
        let source_plan = PlanNode::ReadSource(ReadDataSourcePlan {
            db: "system".to_string(),
            table: "test".to_string(),
            table_id: 0,
            table_version: None,
            schema: DataSchemaRefExt::create(vec![
                DataField::new("a", DataType::Utf8, false),
                DataField::new("b", DataType::Utf8, false),
                DataField::new("c", DataType::Utf8, false),
            ]),
            parts: generate_partitions(8, total as u64),
            statistics: statistics.clone(),
            description: format!(
                "(Read from system.{} table, Read Rows:{}, Read Bytes:{})",
                "test".to_string(),
                statistics.read_rows,
                statistics.read_bytes
            ),
            scan_plan: Arc::new(ScanPlan::empty()),
            remote: false,
        });

        let aggr_expr = Expression::AggregateFunction {
            op: "count".to_string(),
            distinct: false,
            args: vec![Expression::create_literal(DataValue::UInt64(Some(0)))],
        };

        let plan = PlanBuilder::from(&source_plan)
            .expression(
                &[Expression::create_literal(DataValue::UInt64(Some(0)))],
                "Before GroupBy",
            )?
            .aggregate_partial(&[aggr_expr.clone()], &[])?
            .aggregate_final(source_plan.schema(), &[aggr_expr], &[])?
            .project(&[Expression::Column("count(0)".to_string())])?
            .build()?;

        let mut statistics_exact = StatisticsExactOptimizer::create(ctx);
        let optimized = statistics_exact.optimize(&plan)?;

        let expect = "\
        Projection: count(0):UInt64\
        \n  AggregatorFinal: groupBy=[[]], aggr=[[count(0)]]\
        \n    Projection: {\"Struct\":[{\"UInt64\":10000}]} as count(0):Utf8\
        \n      Expression: {\"Struct\":[{\"UInt64\":10000}]}:Utf8 (Exact Statistics)\
        \n        ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 1, read_bytes: 1]";
        let actual = format!("{:?}", optimized);
        assert_eq!(expect, actual);
        Ok(())
    }
}
