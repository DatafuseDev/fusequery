//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::ops::Sub;
use std::time::Duration;

use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storages_fuse::io::SnapshotHistoryReader;
use databend_query::storages::fuse::io::MetaReaders;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::FuseTable;
use databend_query::test_kits::*;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_navigate() -> Result<()> {
    // - perform two insertions, which will left 2 snapshots
    // - navigate to the snapshot generated by the first insertion should be success
    // - navigate to the snapshot that generated before the first insertion should fail

    // 1. Setup
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    // 1.1 first commit
    let qry = format!(
        "insert into {}.{} values (1, (2, 3)), (2, (4, 6)) ",
        db, tbl
    );
    fixture
        .execute_query(qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;

    // keep the first snapshot of the insertion
    let table = fixture.latest_default_table().await?;
    let first_snapshot = FuseTable::try_from_table(table.as_ref())?
        .snapshot_loc()
        .await?
        .unwrap();

    // take a nap
    tokio::time::sleep(Duration::from_millis(2)).await;

    // 1.2 second commit
    let qry = format!("insert into {}.{} values (3, (6, 9)) ", db, tbl);
    fixture
        .execute_query(qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    // keep the snapshot of the second insertion
    let table = fixture.latest_default_table().await?;
    let second_snapshot = FuseTable::try_from_table(table.as_ref())?
        .snapshot_loc()
        .await?
        .unwrap();
    assert_ne!(second_snapshot, first_snapshot);

    // 2. grab the history
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let loc = fuse_table.snapshot_loc().await?.unwrap();
    assert_eq!(second_snapshot, loc);
    let version = TableMetaLocationGenerator::snapshot_version(loc.as_str());
    let snapshots: Vec<_> = reader
        .snapshot_history(
            loc.clone(),
            version,
            fuse_table.meta_location_generator().clone(),
        )
        .try_collect()
        .await?;

    // 3. there should be two snapshots
    assert_eq!(2, snapshots.len());

    // 4. navigate to the first snapshot
    // history is order by timestamp DESC
    let (latest, _ver) = &snapshots[0];
    let instant = latest
        .timestamp
        .unwrap()
        .sub(chrono::Duration::milliseconds(1));
    // navigate from the instant that is just one ms before the timestamp of the latest snapshot
    let tbl = fuse_table
        .navigate_to_time_point(loc.clone(), instant)
        .await?;

    // check we got the snapshot of the first insertion
    assert_eq!(first_snapshot, tbl.snapshot_loc().await?.unwrap());

    // 4. navigate beyond the first snapshot
    let (first_insertion, _ver) = &snapshots[1];
    let instant = first_insertion
        .timestamp
        .unwrap()
        .sub(chrono::Duration::milliseconds(1));
    // navigate from the instant that is just one ms before the timestamp of the last insertion
    let res = fuse_table.navigate_to_time_point(loc, instant).await;
    match res {
        Ok(_) => panic!("historical data should not exist"),
        Err(e) => assert_eq!(e.code(), ErrorCode::TABLE_HISTORICAL_DATA_NOT_FOUND),
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_navigate_for_purge() -> Result<()> {
    // 1. Setup
    let fixture = TestFixture::setup().await?;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    // 1.1 first commit
    let qry = format!(
        "insert into {}.{} values (1, (2, 3)), (2, (4, 6)) ",
        db, tbl
    );
    fixture
        .execute_query(qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;

    // keep the first snapshot of the insertion
    let table = fixture.latest_default_table().await?;
    let first_snapshot = FuseTable::try_from_table(table.as_ref())?
        .snapshot_loc()
        .await?
        .unwrap();

    // take a nap
    tokio::time::sleep(Duration::from_millis(2)).await;

    // 1.2 second commit
    let qry = format!("insert into {}.{} values (3, (6, 9)) ", db, tbl);
    fixture
        .execute_query(qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    // keep the snapshot of the second insertion
    let table = fixture.latest_default_table().await?;
    let second_snapshot = FuseTable::try_from_table(table.as_ref())?
        .snapshot_loc()
        .await?
        .unwrap();

    // take a nap
    tokio::time::sleep(Duration::from_millis(2)).await;

    // 1.3 third commit
    let qry = format!("insert into {}.{} values (4, (8, 12)) ", db, tbl);
    fixture
        .execute_query(qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let table = fixture.latest_default_table().await?;
    let third_snapshot = FuseTable::try_from_table(table.as_ref())?
        .snapshot_loc()
        .await?
        .unwrap();

    // 2. grab the history
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());
    let loc = fuse_table.snapshot_loc().await?.unwrap();
    assert_eq!(third_snapshot, loc);
    let version = TableMetaLocationGenerator::snapshot_version(loc.as_str());
    let snapshots: Vec<_> = reader
        .snapshot_history(
            loc.clone(),
            version,
            fuse_table.meta_location_generator().clone(),
        )
        .try_collect()
        .await?;

    // 3. there should be three snapshots
    assert_eq!(3, snapshots.len());

    // 4. navigate by the time point
    let meta = fuse_table.get_operator().stat(&loc).await?;
    let modified = meta.last_modified();
    assert!(modified.is_some());
    let time_point = modified.unwrap().sub(chrono::Duration::milliseconds(1));
    // navigate from the instant that is just one ms before the timestamp of the latest snapshot.
    let (navigate, files) = fuse_table.list_by_time_point(time_point).await?;
    assert_eq!(2, files.len());
    assert_eq!(navigate, first_snapshot);

    // 5. navigate by snapshot id.
    let snapshot_id = snapshots[1].0.snapshot_id.simple().to_string();
    let (navigate, files) = fuse_table
        .list_by_snapshot_id(&snapshot_id, time_point)
        .await?;
    assert_eq!(2, files.len());
    assert_eq!(navigate, second_snapshot);

    Ok(())
}
