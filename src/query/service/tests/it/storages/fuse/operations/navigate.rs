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

use common_base::base::tokio;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_storages_fuse::io::SnapshotHistoryReader;
use databend_query::storages::fuse::io::MetaReaders;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::FuseTable;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_navigate() -> Result<()> {
    // - perform two insertions, which will left 2 snapshots
    // - navigate to the snapshot generated by the first insertion should be success
    // - navigate to the snapshot that generated before the first insertion should fail

    // 1. Setup
    let fixture = TestFixture::new().await;
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let ctx = fixture.ctx();
    fixture.create_default_table().await?;

    // 1.1 first commit
    let qry = format!(
        "insert into {}.{} values (1, (2, 3)), (2, (4, 6)) ",
        db, tbl
    );
    execute_query(ctx.clone(), qry.as_str())
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
    execute_query(ctx.clone(), qry.as_str())
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
        .snapshot_history(loc, version, fuse_table.meta_location_generator().clone())
        .try_collect()
        .await?;

    // 3. there should be two snapshots
    assert_eq!(2, snapshots.len());

    // 4. navigate to the first snapshot
    // history is order by timestamp DESC
    let latest = &snapshots[0];
    let instant = latest
        .timestamp
        .unwrap()
        .sub(chrono::Duration::milliseconds(1));
    // navigate from the instant that is just one ms before the timestamp of the latest snapshot
    let tbl = fuse_table.navigate_to_time_point(instant).await?;

    // check we got the snapshot of the first insertion
    assert_eq!(first_snapshot, tbl.snapshot_loc().await?.unwrap());

    // 4. navigate beyond the first snapshot
    let first_insertion = &snapshots[1];
    let instant = first_insertion
        .timestamp
        .unwrap()
        .sub(chrono::Duration::milliseconds(1));
    // navigate from the instant that is just one ms before the timestamp of the last insertion
    let res = fuse_table.navigate_to_time_point(instant).await;
    match res {
        Ok(_) => panic!("historical data should not exist"),
        Err(e) => assert_eq!(e.code(), ErrorCode::TABLE_HISTORICAL_DATA_NOT_FOUND),
    };
    Ok(())
}
