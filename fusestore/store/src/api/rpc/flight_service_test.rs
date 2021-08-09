// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_flights::meta_api_impl::DropTableActionResult;
use common_flights::meta_api_impl::GetTableActionResult;
use common_flights::KVApi;
use common_flights::MetaApi;
use common_flights::StorageApi;
use common_flights::StoreClient;
use common_metatypes::MatchSeq;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::DatabaseEngineType;
use common_planners::DropDatabasePlan;
use common_planners::DropTablePlan;
use common_planners::ScanPlan;
use common_planners::TableEngineType;
use common_runtime::tokio;
use common_tracing::tracing;
use pretty_assertions::assert_eq;

use crate::tests::service::init_store_unittest;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_restart() -> anyhow::Result<()> {
    // Issue 1134  https://github.com/datafuselabs/datafuse/issues/1134
    // - Start a store server.
    // - create db and create table
    // - restart
    // - Test read the db and read the table.

    init_store_unittest();

    let (mut tc, addr) = crate::tests::start_store_server().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    let db_name = "db1";
    let table_name = "table1";

    tracing::info!("--- create db");
    {
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;
        tracing::debug!("create database res: {:?}", res);
        let res = res?;
        assert_eq!(1, res.database_id, "first database id is 1");
    }

    tracing::info!("--- get db");
    {
        let res = client.get_database(db_name).await;
        tracing::debug!("get present database res: {:?}", res);
        let res = res?;
        assert_eq!(1, res.database_id, "db1 id is 1");
        assert_eq!(db_name, res.db, "db1.db is db1");
    }

    tracing::info!("--- create table {}.{}", db_name, table_name);
    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    {
        let plan = CreateTablePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            table: table_name.to_string(),
            schema: schema.clone(),
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            engine: TableEngineType::JSONEachRow,
        };

        {
            let res = client.create_table(plan.clone()).await?;
            assert_eq!(1, res.table_id, "table id is 1");

            let got = client.get_table(db_name.into(), table_name.into()).await?;
            let want = GetTableActionResult {
                table_id: 1,
                db: db_name.into(),
                name: table_name.into(),
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get created table");
        }
    }

    tracing::info!("--- stop StoreServer");
    {
        let (stop_tx, fin_rx) = tc.channels.take().unwrap();
        stop_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("fail to send"))?;

        fin_rx.await?;

        drop(client);

        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // restart by opening existent meta db
        tc.config.boot = false;
        crate::tests::start_store_server_with_context(&mut tc).await?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(10_000)).await;

    // try to reconnect the restarted server.
    let mut _client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    // TODO(xp): db and table are still in pure memory store. the following test will no pass.

    // tracing::info!("--- get db");
    // {
    //     let res = client.get_database(db_name).await;
    //     tracing::debug!("get present database res: {:?}", res);
    //     let res = res?;
    //     assert_eq!(1, res.database_id, "db1 id is 1");
    //     assert_eq!(db_name, res.db, "db1.db is db1");
    // }
    //
    // tracing::info!("--- get table");
    // {
    //     let got = client
    //         .get_table(db_name.into(), table_name.into())
    //         .await
    //         .unwrap();
    //     let want = GetTableActionResult {
    //         table_id: 1,
    //         db: db_name.into(),
    //         name: table_name.into(),
    //         schema: schema.clone(),
    //     };
    //     assert_eq!(want, got, "get created table");
    // }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_database() -> anyhow::Result<()> {
    init_store_unittest();

    // 1. Service starts.
    let (_tc, addr) = crate::tests::start_store_server().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    // 2. Create database.

    // TODO: test arg if_not_exists: It should respond  an ErrorCode
    {
        // create first db
        let plan = CreateDatabasePlan {
            // TODO test if_not_exists
            if_not_exists: false,
            db: "db1".to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;
        tracing::info!("create database res: {:?}", res);
        let res = res.unwrap();
        assert_eq!(1, res.database_id, "first database id is 1");
    }
    {
        // create second db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: "db2".to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;
        tracing::info!("create database res: {:?}", res);
        let res = res.unwrap();
        assert_eq!(2, res.database_id, "second database id is 2");
    }

    // 3. Get database.

    {
        // get present db
        let res = client.get_database("db1").await;
        tracing::debug!("get present database res: {:?}", res);
        let res = res?;
        assert_eq!(1, res.database_id, "db1 id is 1");
        assert_eq!("db1".to_string(), res.db, "db1.db is db1");
    }

    {
        // get absent db
        let res = client.get_database("ghost").await;
        tracing::debug!("=== get absent database res: {:?}", res);
        assert!(res.is_err());
        let res = res.unwrap_err();
        assert_eq!(3, res.code());
        assert_eq!("ghost".to_string(), res.message());
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_create_get_table() -> anyhow::Result<()> {
    init_store_unittest();
    use std::sync::Arc;

    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    tracing::info!("init logging");

    // 1. Service starts.
    let (_tc, addr) = crate::tests::start_store_server().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    let db_name = "db1";
    let tbl_name = "tb2";

    {
        // prepare db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;

        tracing::info!("create database res: {:?}", res);

        let res = res.unwrap();
        assert_eq!(1, res.database_id, "first database id is 1");
    }
    {
        // create table and fetch it

        // Table schema with metadata(due to serde issue).
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "number",
            DataType::UInt64,
            false,
        )]));

        // Create table plan.
        let mut plan = CreateTablePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            table: tbl_name.to_string(),
            schema: schema.clone(),
            // TODO check get_table
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            // TODO
            engine: TableEngineType::JSONEachRow,
        };

        {
            // create table OK
            let res = client.create_table(plan.clone()).await.unwrap();
            assert_eq!(1, res.table_id, "table id is 1");

            let got = client
                .get_table(db_name.into(), tbl_name.into())
                .await
                .unwrap();
            let want = GetTableActionResult {
                table_id: 1,
                db: db_name.into(),
                name: tbl_name.into(),
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get created table");
        }

        {
            // create table again with if_not_exists = true
            plan.if_not_exists = true;
            let res = client.create_table(plan.clone()).await.unwrap();
            assert_eq!(1, res.table_id, "new table id");

            let got = client
                .get_table(db_name.into(), tbl_name.into())
                .await
                .unwrap();
            let want = GetTableActionResult {
                table_id: 1,
                db: db_name.into(),
                name: tbl_name.into(),
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get created table");
        }

        {
            // create table again with if_not_exists=false
            plan.if_not_exists = false;

            let res = client.create_table(plan.clone()).await;
            tracing::info!("create table res: {:?}", res);

            let status = res.err().unwrap();
            assert_eq!(
                format!("Code: 4003, displayText = table exists: {}.", tbl_name),
                status.to_string()
            );

            // get_table returns the old table

            let got = client.get_table("db1".into(), "tb2".into()).await.unwrap();
            let want = GetTableActionResult {
                table_id: 1,
                db: db_name.into(),
                name: tbl_name.into(),
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get old table");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_drop_table() -> anyhow::Result<()> {
    init_store_unittest();
    use std::sync::Arc;

    use common_datavalues::DataField;
    use common_datavalues::DataSchema;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    tracing::info!("init logging");

    // 1. Service starts.
    let (_tc, addr) = crate::tests::start_store_server().await?;

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    let db_name = "db1";
    let tbl_name = "tb2";

    {
        // prepare db
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };

        let res = client.create_database(plan.clone()).await;

        tracing::info!("create database res: {:?}", res);

        let res = res.unwrap();
        assert_eq!(1, res.database_id, "first database id is 1");
    }
    {
        // create table and fetch it

        // Table schema with metadata(due to serde issue).
        let schema = Arc::new(DataSchema::new(vec![DataField::new(
            "number",
            DataType::UInt64,
            false,
        )]));

        // Create table plan.
        let plan = CreateTablePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            table: tbl_name.to_string(),
            schema: schema.clone(),
            // TODO check get_table
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            // TODO
            engine: TableEngineType::JSONEachRow,
        };

        {
            // create table OK
            let res = client.create_table(plan.clone()).await.unwrap();
            assert_eq!(1, res.table_id, "table id is 1");

            let got = client
                .get_table(db_name.into(), tbl_name.into())
                .await
                .unwrap();
            let want = GetTableActionResult {
                table_id: 1,
                db: db_name.into(),
                name: tbl_name.into(),
                schema: schema.clone(),
            };
            assert_eq!(want, got, "get created table");
        }

        {
            // drop table
            let plan = DropTablePlan {
                if_exists: true,
                db: db_name.to_string(),
                table: tbl_name.to_string(),
            };
            let res = client.drop_table(plan.clone()).await.unwrap();
            assert_eq!(DropTableActionResult {}, res, "drop table {}", tbl_name)
        }

        {
            let res = client.get_table(db_name.into(), tbl_name.into()).await;
            let status = res.err().unwrap();
            assert_eq!(
                format!("Code: 25, displayText = table not found: {}.", tbl_name),
                status.to_string(),
                "get dropped table {}",
                tbl_name
            );
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_do_append() -> anyhow::Result<()> {
    init_store_unittest();

    use std::sync::Arc;

    use common_datavalues::prelude::*;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    let (_tc, addr) = crate::tests::start_store_server().await?;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("col_i", DataType::Int64, false),
        DataField::new("col_s", DataType::Utf8, false),
    ]));
    let db_name = "test_db";
    let tbl_name = "test_tbl";

    let series0 = Series::new(vec![0i64, 1, 2]);
    let series1 = Series::new(vec!["str1", "str2", "str3"]);

    let expected_rows = series0.len() * 2;
    let expected_cols = 2;

    let block = DataBlock::create_by_array(schema.clone(), vec![series0, series1]);
    let batches = vec![block.clone(), block];
    let num_batch = batches.len();
    let stream = futures::stream::iter(batches);

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;
    {
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };
        let res = client.create_database(plan.clone()).await;
        let res = res.unwrap();
        assert_eq!(res.database_id, 1, "db created");
        let plan = CreateTablePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            table: tbl_name.to_string(),
            schema: schema.clone(),
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            engine: TableEngineType::Parquet,
        };
        client.create_table(plan.clone()).await.unwrap();
    }
    let res = client
        .append_data(
            db_name.to_string(),
            tbl_name.to_string(),
            schema,
            Box::pin(stream),
        )
        .await
        .unwrap();
    tracing::info!("append res is {:?}", res);
    let summary = res.summary;
    assert_eq!(summary.rows, expected_rows, "rows eq");
    assert_eq!(res.parts.len(), num_batch, "batch eq");
    res.parts.iter().for_each(|p| {
        assert_eq!(p.rows, expected_rows / num_batch);
        assert_eq!(p.cols, expected_cols);
    });
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_scan_partition() -> anyhow::Result<()> {
    init_store_unittest();
    use std::sync::Arc;

    use common_datavalues::prelude::*;
    use common_flights::StoreClient;
    use common_planners::CreateDatabasePlan;
    use common_planners::CreateTablePlan;
    use common_planners::DatabaseEngineType;
    use common_planners::TableEngineType;

    let (_tc, addr) = crate::tests::start_store_server().await?;

    let schema = Arc::new(DataSchema::new(vec![
        DataField::new("col_i", DataType::Int64, false),
        DataField::new("col_s", DataType::Utf8, false),
    ]));
    let db_name = "test_db";
    let tbl_name = "test_tbl";

    let series0 = Series::new(vec![0i64, 1, 2]);
    let series1 = Series::new(vec!["str1", "str2", "str3"]);

    let rows_of_series0 = series0.len();
    let rows_of_series1 = series1.len();
    let expected_rows = rows_of_series0 + rows_of_series1;
    let expected_cols = 2;

    let block = DataBlock::create(schema.clone(), vec![
        DataColumn::Array(series0),
        DataColumn::Array(series1),
    ]);
    let batches = vec![block.clone(), block];
    let num_batch = batches.len();
    let stream = futures::stream::iter(batches);

    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;
    {
        let plan = CreateDatabasePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            engine: DatabaseEngineType::Local,
            options: Default::default(),
        };
        client.create_database(plan.clone()).await?;
        let plan = CreateTablePlan {
            if_not_exists: false,
            db: db_name.to_string(),
            table: tbl_name.to_string(),
            schema: schema.clone(),
            options: maplit::hashmap! {"opt‐1".into() => "val-1".into()},
            engine: TableEngineType::Parquet,
        };
        client.create_table(plan.clone()).await?;
    }
    let res = client
        .append_data(
            db_name.to_string(),
            tbl_name.to_string(),
            schema,
            Box::pin(stream),
        )
        .await?;
    tracing::info!("append res is {:?}", res);
    let summary = res.summary;
    assert_eq!(summary.rows, expected_rows);
    assert_eq!(res.parts.len(), num_batch);
    res.parts.iter().for_each(|p| {
        assert_eq!(p.rows, expected_rows / num_batch);
        assert_eq!(p.cols, expected_cols);
    });

    log::debug!("summary is {:?}", summary);

    let plan = ScanPlan {
        schema_name: tbl_name.to_string(),
        ..ScanPlan::empty()
    };
    let res = client
        .read_plan(db_name.to_string(), tbl_name.to_string(), &plan)
        .await;

    assert!(res.is_ok());
    let read_plan_res = res.unwrap();
    assert!(read_plan_res.is_some());
    let read_plan = read_plan_res.unwrap();
    assert_eq!(2, read_plan.len());
    assert_eq!(read_plan[0].stats.read_rows, rows_of_series0);
    assert_eq!(read_plan[1].stats.read_rows, rows_of_series1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_mget() -> anyhow::Result<()> {
    init_store_unittest();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_store_server().await?;

        let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        client
            .upsert_kv("k1", MatchSeq::Any, b"v1".to_vec())
            .await?;
        client
            .upsert_kv("k2", MatchSeq::Any, b"v2".to_vec())
            .await?;

        let res = client
            .mget_kv(&vec!["k1".to_string(), "k2".to_string()])
            .await?;
        assert_eq!(res.result, vec![
            Some((1, b"v1".to_vec())),
            // NOTE, the sequence number is increased globally (inside the namespace of generic kv)
            Some((2, b"v2".to_vec())),
        ]);

        let res = client
            .mget_kv(&vec!["k1".to_string(), "key_no exist".to_string()])
            .await?;
        assert_eq!(res.result, vec![Some((1, b"v1".to_vec())), None]);
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_list() -> anyhow::Result<()> {
    init_store_unittest();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_store_server().await?;

        let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let mut values = vec![];
        {
            client
                .upsert_kv("t", MatchSeq::Any, "".as_bytes().to_vec())
                .await?;

            for i in 0..9 {
                let key = format!("__users/{}", i);
                let val = format!("val_{}", i);
                values.push(val.clone());
                client
                    .upsert_kv(&key, MatchSeq::Any, val.as_bytes().to_vec())
                    .await?;
            }
            client
                .upsert_kv("v", MatchSeq::Any, "".as_bytes().to_vec())
                .await?;
        }

        let res = client.prefix_list_kv("__users/").await?;
        assert_eq!(
            res.iter()
                .map(|(_key, (_s, val))| val.clone())
                .collect::<Vec<_>>(),
            values
                .iter()
                .map(|v| v.as_bytes().to_vec())
                .collect::<Vec<_>>()
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_delete() -> anyhow::Result<()> {
    init_store_unittest();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_store_server().await?;

        let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let test_key = "test_key";
        client
            .upsert_kv(test_key, MatchSeq::Any, b"v1".to_vec())
            .await?;

        let current = client.get_kv(test_key).await?;
        if let Some((seq, _val)) = current.result {
            // seq mismatch
            let wrong_seq = Some(seq + 1);
            let res = client.delete_kv(test_key, wrong_seq).await?;
            assert!(res.is_none());

            // seq match
            let res = client.delete_kv(test_key, Some(seq)).await?;
            assert!(res.is_some());

            // read nothing
            let r = client.get_kv(test_key).await?;
            assert!(r.result.is_none());
        } else {
            panic!("expecting a value, but got nothing");
        }

        // key not exist
        let res = client.delete_kv("not exists", None).await?;
        assert!(res.is_none());

        // do not care seq
        client
            .upsert_kv(test_key, MatchSeq::Any, b"v2".to_vec())
            .await?;

        let res = client.delete_kv(test_key, None).await?;
        assert_eq!(Some((2, b"v2".to_vec())), res);
    }
    Ok(())
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv_update() -> anyhow::Result<()> {
    init_store_unittest();
    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv_list");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_store_server().await?;

        let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        let test_key = "test_key_for_update";

        let r = client
            .upsert_kv(test_key, MatchSeq::GE(1), b"v1".to_vec())
            .await?;
        assert_eq!((None, None), (r.prev, r.result), "not changed");

        let r = client
            .upsert_kv(test_key, MatchSeq::Any, b"v1".to_vec())
            .await?;
        assert_eq!(Some((1, b"v1".to_vec())), r.result);
        let seq = r.result.unwrap().0;

        // unmatched seq
        let r = client
            .upsert_kv(test_key, MatchSeq::Exact(seq + 1), b"v2".to_vec())
            .await?;
        assert_eq!(Some((1, b"v1".to_vec())), r.prev);
        assert_eq!(Some((1, b"v1".to_vec())), r.result);

        // matched seq
        let r = client
            .upsert_kv(test_key, MatchSeq::Exact(seq), b"v2".to_vec())
            .await?;
        assert_eq!(Some((1, b"v1".to_vec())), r.prev);
        assert_eq!(Some((2, b"v2".to_vec())), r.result);

        // blind update
        let r = client
            .upsert_kv(test_key, MatchSeq::GE(1), b"v3".to_vec())
            .await?;
        assert_eq!(Some((2, b"v2".to_vec())), r.prev);
        assert_eq!(Some((3, b"v3".to_vec())), r.result);

        // value updated
        let kv = client.get_kv(test_key).await?;
        assert!(kv.result.is_some());
        assert_eq!(kv.result.unwrap().1, b"v3".to_vec());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_generic_kv() -> anyhow::Result<()> {
    init_store_unittest();

    {
        let span = tracing::span!(tracing::Level::INFO, "test_flight_generic_kv");
        let _ent = span.enter();

        let (_tc, addr) = crate::tests::start_store_server().await?;

        let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

        {
            // write
            let res = client
                .upsert_kv("foo", MatchSeq::Any, "bar".to_string().into_bytes())
                .await?;
            assert_eq!(None, res.prev);
            assert_eq!(Some((1, "bar".to_string().into_bytes())), res.result);
        }

        {
            // write fails with unmatched seq
            let res = client
                .upsert_kv("foo", MatchSeq::Exact(2), b"bar".to_vec())
                .await?;
            assert_eq!(
                (Some((1, b"bar".to_vec())), Some((1, b"bar".to_vec())),),
                (res.prev, res.result),
                "nothing changed"
            );
        }

        {
            // write done with matching seq
            let res = client
                .upsert_kv("foo", MatchSeq::Exact(1), b"wow".to_vec())
                .await?;
            assert_eq!(Some((1, b"bar".to_vec())), res.prev, "old value");
            assert_eq!(Some((2, b"wow".to_vec())), res.result, "new value");
        }
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_get_database_meta_empty_db() -> anyhow::Result<()> {
    init_store_unittest();
    let (_tc, addr) = crate::tests::start_store_server().await?;
    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    // Empty Database
    let res = client.get_database_meta(None).await?;
    assert!(res.is_none());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_get_database_meta_ddl_db() -> anyhow::Result<()> {
    init_store_unittest();
    let (_tc, addr) = crate::tests::start_store_server().await?;
    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    // create-db operation will increases meta_version
    let plan = CreateDatabasePlan {
        if_not_exists: false,
        db: "db1".to_string(),
        engine: DatabaseEngineType::Local,
        options: Default::default(),
    };
    client.create_database(plan).await?;

    let res = client.get_database_meta(None).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(1, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());

    // if lower_bound < current meta version, returns database meta
    let res = client.get_database_meta(Some(0)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(1, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());

    // if lower_bound equals current meta version, returns None
    let res = client.get_database_meta(Some(1)).await?;
    assert!(res.is_none());

    // failed ddl do not effect meta version
    let plan = CreateDatabasePlan {
        if_not_exists: true, // <<--
        db: "db1".to_string(),
        engine: DatabaseEngineType::Local, // accepts a Local engine?
        options: Default::default(),
    };

    client.create_database(plan).await?;
    let res = client.get_database_meta(Some(1)).await?;
    assert!(res.is_none());

    // drop-db will increase meta version
    let plan = DropDatabasePlan {
        if_exists: true,
        db: "db1".to_string(),
    };

    client.drop_database(plan).await?;
    let res = client.get_database_meta(Some(1)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();

    assert_eq!(2, snapshot.meta_ver);
    assert_eq!(0, snapshot.db_metas.len());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_flight_get_database_meta_ddl_table() -> anyhow::Result<()> {
    init_store_unittest();
    let (_tc, addr) = crate::tests::start_store_server().await?;
    let mut client = StoreClient::try_create(addr.as_str(), "root", "xxx").await?;

    let test_db = "db1";
    let plan = CreateDatabasePlan {
        if_not_exists: false,
        db: test_db.to_string(),
        engine: DatabaseEngineType::Local,
        options: Default::default(),
    };
    client.create_database(plan).await?;

    // After `create db`, meta_ver will be increased to 1

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "number",
        DataType::UInt64,
        false,
    )]));

    // create-tbl operation will increases meta_version
    let plan = CreateTablePlan {
        if_not_exists: true,
        db: test_db.to_string(),
        table: "tbl1".to_string(),
        schema: schema.clone(),
        options: Default::default(),
        engine: TableEngineType::JSONEachRow,
    };

    client.create_table(plan.clone()).await?;

    let res = client.get_database_meta(None).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(2, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());
    assert_eq!(1, snapshot.tbl_metas.len());

    // if lower_bound < current meta version, returns database meta
    let res = client.get_database_meta(Some(0)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(2, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());

    // if lower_bound equals current meta version, returns None
    let res = client.get_database_meta(Some(2)).await?;
    assert!(res.is_none());

    // failed ddl do not effect meta version
    //  recall: plan.if_not_exist == true
    let _r = client.create_table(plan).await?;
    let res = client.get_database_meta(Some(2)).await?;
    assert!(res.is_none());

    // drop-table will increase meta version
    let plan = DropTablePlan {
        if_exists: true,
        db: test_db.to_string(),
        table: "tbl1".to_string(),
    };

    client.drop_table(plan).await?;
    let res = client.get_database_meta(Some(2)).await?;
    assert!(res.is_some());
    let snapshot = res.unwrap();
    assert_eq!(3, snapshot.meta_ver);
    assert_eq!(1, snapshot.db_metas.len());
    assert_eq!(0, snapshot.tbl_metas.len());

    Ok(())
}
