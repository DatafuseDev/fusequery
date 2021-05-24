// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use pretty_assertions::assert_eq;

use crate::sql::PlanParser;

#[test]
fn test_plan_parser() -> anyhow::Result<()> {
    struct Test {
        name: &'static str,
        sql: &'static str,
        expect: &'static str,
        error: &'static str,
    }

    let tests = vec![
        Test {
            name: "create-database-passed",
            sql: "CREATE DATABASE db1",
            expect: "Create database db1, engine: Remote, if_not_exists:false, option: {}",
            error: "",
        },
        Test {
            name: "create-database-if-not-exists-passed",
            sql: "CREATE DATABASE IF NOT EXISTS db1",
            expect: "Create database db1, engine: Remote, if_not_exists:true, option: {}",
            error: "",
        },
        Test {
            name: "drop-database-passed",
            sql: "DROP DATABASE db1",
            expect: "Drop database db1, if_exists:false",
            error: "",
        },
        Test {
            name: "drop-database-if-exists-passed",
            sql: "DROP DATABASE IF EXISTS db1",
            expect: "Drop database db1, if_exists:true",
            error: "",
        },
        Test {
            name: "create-table-passed",
            sql: "CREATE TABLE t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' ",
            expect: "Create table default.t Field { name: \"c1\", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c2\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, engine: Parquet, if_not_exists:false, option: {\"location\": \"'foo.parquet'\"}",
            error: "",
        },
        Test {
            name: "create-table-if-not-exists-passed",
            sql: "CREATE TABLE IF NOT EXISTS t(c1 int, c2 bigint, c3 varchar(255) ) ENGINE = Parquet location = 'foo.parquet' ",
            expect: "Create table default.t Field { name: \"c1\", data_type: Int32, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c2\", data_type: Int64, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, Field { name: \"c3\", data_type: Utf8, nullable: false, dict_id: 0, dict_is_ordered: false, metadata: None }, engine: Parquet, if_not_exists:true, option: {\"location\": \"'foo.parquet'\"}",
            error: "",
        },
        Test {
            name: "drop-table-passed",
            sql: "DROP TABLE t1",
            expect: "Drop table default.t1, if_exists:false",
            error: "",
        },
        Test {
            name: "drop-table-passed",
            sql: "DROP TABLE db1.t1",
            expect: "Drop table db1.t1, if_exists:false",
            error: "",
        },
        Test {
            name: "drop-table-if-exists-passed",
            sql: "DROP TABLE IF EXISTS db1.t1",
            expect: "Drop table db1.t1, if_exists:true",
            error: "",
        },
        Test {
        name: "cast-passed",
        sql: "select cast('1' as int)",
        expect: "Projection: cast(1 as Int32):Int32\n  Expression: cast(1 as Int32):UInt8 (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
        },
        Test {
        name: "database-passed",
        sql: "select database()",
        expect: "Projection: database([default]):Utf8\n  Expression: database([default]):UInt8 (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
        error: "",
        },
        Test {
            name: "unsupported-function",
            sql: "select unsupported()",
            expect: "",
            error: "Code: 8, displayText = Unsupported function: \"unsupported\".",
        },
        Test {
            name: "interval-passed",
            sql: "SELECT INTERVAL '1 year', INTERVAL '1 month', INTERVAL '1 day', INTERVAL '1 hour', INTERVAL '1 minute', INTERVAL '1 second'",
            expect: "Projection: 12:Interval(YearMonth), 1:Interval(YearMonth), 4294967296:Interval(DayTime), 3600000:Interval(DayTime), 60000:Interval(DayTime), 1000:Interval(DayTime)\n  Expression: 12:UInt8, 1:Interval(YearMonth), 4294967296:Interval(YearMonth), 3600000:Interval(DayTime), 60000:Interval(DayTime), 1000:Interval(DayTime) (Before Projection)\n    ReadDataSource: scan partitions: [1], scan schema: [dummy:UInt8], statistics: [read_rows: 0, read_bytes: 0]",
            error: ""
        },
        Test {
            name: "interval-unsupported",
            sql: "SELECT INTERVAL '1 year 1 day'",
            expect: "",
            error: "Code: 5, displayText = DF does not support intervals that have both a Year/Month part as well as Days/Hours/Mins/Seconds: \"1 year 1 day\". Hint: try breaking the interval into two parts, one with Year/Month and the other with Days/Hours/Mins/Seconds - e.g. (NOW() + INTERVAL \'1 year\') + INTERVAL \'1 day\'."
        },
        Test {
            name: "interval-out-of-range",
            sql: "SELECT INTERVAL '100000000000000000 day'",
            expect: "",
            error: "Code: 5, displayText = Interval field value out of range: \"100000000000000000 day\".",
        },
    ];

    let ctx = crate::tests::try_create_context()?;
    for t in tests {
        let plan = PlanParser::create(ctx.clone()).build_from_sql(t.sql);
        match plan {
            Ok(v) => {
                assert_eq!(t.expect, format!("{:?}", v), "{:#}", t.name);
            }
            Err(e) => {
                assert_eq!(t.error, e.to_string(), "{:#}", t.name);
            }
        }
    }

    Ok(())
}
