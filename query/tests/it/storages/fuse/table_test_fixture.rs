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
//

use std::sync::Arc;

use common_datablocks::assert_blocks_sorted_eq_with_name;
use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_datavalues::DataValue;
use common_exception::Result;
use common_meta_types::DatabaseMeta;
use common_meta_types::TableMeta;
use common_planners::CreateDatabasePlan;
use common_planners::CreateTablePlan;
use common_planners::Expression;
use common_planners::Extras;
use common_streams::SendableDataBlockStream;
use databend_query::catalogs::Catalog;
use databend_query::configs::Config;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;
use databend_query::sql::PlanParser;
use databend_query::storages::fuse::FuseHistoryTable;
use databend_query::storages::fuse::FUSE_TBL_BLOCK_PREFIX;
use databend_query::storages::fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_query::storages::fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use databend_query::storages::fuse::TBL_OPT_KEY_CHUNK_BLOCK_NUM;
use databend_query::storages::Table;
use databend_query::storages::ToReadDataSourcePlan;
use databend_query::table_functions::TableArgs;
use futures::TryStreamExt;
use tempfile::TempDir;
use uuid::Uuid;
use walkdir::WalkDir;

pub struct TestFixture {
    _tmp_dir: TempDir,
    ctx: Arc<QueryContext>,
    prefix: String,
}

impl TestFixture {
    pub async fn new() -> TestFixture {
        let tmp_dir = TempDir::new().unwrap();
        let mut config = Config::default();
        // make sure we are suing `Disk` storage
        config.storage.storage_type = "Disk".to_string();
        // use `TempDir` as root path (auto clean)
        config.storage.disk.data_path = tmp_dir.path().to_str().unwrap().to_string();
        config.storage.disk.temp_data_path = tmp_dir.path().to_str().unwrap().to_string();
        let ctx = crate::tests::create_query_context_with_config(config).unwrap();

        let tenant = ctx.get_tenant();
        let random_prefix: String = Uuid::new_v4().simple().to_string();
        // prepare a randomly named default database
        let db_name = gen_db_name(&random_prefix);
        let plan = CreateDatabasePlan {
            tenant,
            if_not_exists: false,
            db: db_name,
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..Default::default()
            },
        };
        ctx.get_catalog()
            .create_database(plan.into())
            .await
            .unwrap();

        Self {
            _tmp_dir: tmp_dir,
            ctx,
            prefix: random_prefix,
        }
    }

    pub fn ctx(&self) -> Arc<QueryContext> {
        self.ctx.clone()
    }

    pub fn default_tenant(&self) -> String {
        self.ctx().get_tenant()
    }

    pub fn default_db_name(&self) -> String {
        gen_db_name(&self.prefix)
    }

    pub fn default_table_name(&self) -> String {
        format!("tbl_{}", self.prefix)
    }

    pub fn default_schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![DataField::new("id", DataType::Int32, false)])
    }

    pub fn default_crate_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            tenant: self.default_tenant(),
            db: self.default_db_name(),
            table: self.default_table_name(),
            table_meta: TableMeta {
                schema: TestFixture::default_schema(),
                engine: "FUSE".to_string(),
                // make sure blocks will not be merged
                options: [(TBL_OPT_KEY_CHUNK_BLOCK_NUM.to_owned(), "1".to_owned())].into(),
                ..Default::default()
            },
            as_select: None,
        }
    }

    pub async fn create_default_table(&self) -> Result<()> {
        let create_table_plan = self.default_crate_table_plan();
        let catalog = self.ctx.get_catalog();
        catalog.create_table(create_table_plan.into()).await
    }

    pub fn gen_sample_blocks(num: u32, start: i32) -> Vec<Result<DataBlock>> {
        (0..num)
            .into_iter()
            .map(|_v| {
                let schema =
                    DataSchemaRefExt::create(vec![DataField::new("a", DataType::Int32, false)]);
                Ok(DataBlock::create_by_array(schema, vec![Series::new(vec![
                    start,
                    start + 1,
                    start + 2,
                ])]))
            })
            .collect()
    }

    pub fn gen_sample_blocks_stream(num: u32, start: i32) -> SendableDataBlockStream {
        let blocks = Self::gen_sample_blocks(num, start);
        Box::pin(futures::stream::iter(blocks))
    }

    pub async fn latest_default_table(&self) -> Result<Arc<dyn Table>> {
        self.ctx
            .get_catalog()
            .get_table(
                self.default_tenant().as_str(),
                self.default_db_name().as_str(),
                self.default_table_name().as_str(),
            )
            .await
    }
}

fn gen_db_name(prefix: &str) -> String {
    format!("db_{}", prefix)
}

pub async fn test_drive(
    test_db: Option<&str>,
    test_tbl: Option<&str>,
) -> Result<SendableDataBlockStream> {
    let arg_db =
        Expression::create_literal(DataValue::String(test_db.map(|v| v.as_bytes().to_vec())));
    let arg_tbl =
        Expression::create_literal(DataValue::String(test_tbl.map(|v| v.as_bytes().to_vec())));
    let tbl_args = Some(vec![arg_db, arg_tbl]);
    test_drive_with_args(tbl_args).await
}

pub async fn test_drive_with_args(tbl_args: TableArgs) -> Result<SendableDataBlockStream> {
    let ctx = crate::tests::create_query_context()?;
    test_drive_with_args_and_ctx(tbl_args, ctx).await
}

pub async fn test_drive_with_args_and_ctx(
    tbl_args: TableArgs,
    ctx: std::sync::Arc<QueryContext>,
) -> Result<SendableDataBlockStream> {
    let func = FuseHistoryTable::create("system", "fuse_history", 1, tbl_args)?;
    let source_plan = func
        .clone()
        .as_table()
        .read_plan(ctx.clone(), Some(Extras::default()))
        .await?;
    ctx.try_set_partitions(source_plan.parts.clone())?;
    func.read(ctx, &source_plan).await
}

pub fn expects_err<T>(case_name: &str, err_code: u16, res: Result<T>) {
    if let Err(err) = res {
        assert_eq!(
            err.code(),
            err_code,
            "case name {}, unexpected error: {}",
            case_name,
            err
        );
    } else {
        panic!(
            "case name {}, expecting err code {}, but got ok",
            case_name, err_code,
        );
    }
}

pub async fn expects_ok(
    case_name: impl AsRef<str>,
    res: Result<SendableDataBlockStream>,
    expected: Vec<&str>,
) -> Result<()> {
    match res {
        Ok(stream) => {
            let blocks: Vec<DataBlock> = stream.try_collect().await.unwrap();
            assert_blocks_sorted_eq_with_name(case_name.as_ref(), expected, &blocks)
        }
        Err(err) => {
            panic!(
                "case name {}, expecting  Ok, but got err {}",
                case_name.as_ref(),
                err,
            )
        }
    };
    Ok(())
}

pub async fn execute_query(query: &str, ctx: Arc<QueryContext>) -> Result<SendableDataBlockStream> {
    let plan = PlanParser::parse(query, ctx.clone()).await?;
    InterpreterFactory::get(ctx.clone(), plan)?
        .execute(None)
        .await
}

pub async fn execute_command(query: &str, ctx: Arc<QueryContext>) -> Result<()> {
    let res = execute_query(query, ctx).await?;
    res.try_collect::<Vec<DataBlock>>().await?;
    Ok(())
}

pub async fn append_sample_data(num_blocks: u32, fixture: &TestFixture) -> Result<()> {
    append_sample_data_overwrite(num_blocks, false, fixture).await
}

pub async fn append_sample_data_overwrite(
    num_blocks: u32,
    overwrite: bool,
    fixture: &TestFixture,
) -> Result<()> {
    let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;
    let ctx = fixture.ctx();
    let stream = table.append_data(ctx.clone(), stream).await?;
    table
        .commit_insertion(ctx, stream.try_collect().await?, overwrite)
        .await
}

pub async fn check_data_dir(
    fixture: &TestFixture,
    case_name: &str,
    snapshot_count: u32,
    segment_count: u32,
    block_count: u32,
) {
    let data_path = fixture.ctx().get_config().storage.disk.data_path;
    let root = data_path.as_str();
    let mut ss_count = 0;
    let mut sg_count = 0;
    let mut b_count = 0;
    // avoid ugly line wrapping
    let prefix_snapshot = FUSE_TBL_SNAPSHOT_PREFIX;
    let prefix_segment = FUSE_TBL_SEGMENT_PREFIX;
    let prefix_block = FUSE_TBL_BLOCK_PREFIX;
    for entry in WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            // here, by checking if "contains" the prefix is enough
            if entry.path().to_str().unwrap().contains(prefix_snapshot) {
                ss_count += 1;
            } else if entry.path().to_str().unwrap().contains(prefix_segment) {
                sg_count += 1;
            } else if entry.path().to_str().unwrap().contains(prefix_block) {
                b_count += 1;
            }
        }
    }

    assert_eq!(
        ss_count, snapshot_count,
        "case [{}], check snapshot count",
        case_name
    );
    assert_eq!(
        sg_count, segment_count,
        "case [{}], check segment count",
        case_name
    );

    assert_eq!(
        b_count, block_count,
        "case [{}], check block count",
        case_name
    );
}

pub async fn history_should_have_only_one_item(
    fixture: &TestFixture,
    case_name: &str,
) -> Result<()> {
    // check history
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let expected = vec![
        "+-------+",
        "| count |",
        "+-------+",
        "| 1     |",
        "+-------+",
    ];
    let qry = format!(
        "select count(*) as count from fuse_history('{}', '{}')",
        db, tbl
    );

    expects_ok(
        format!("{}: count_of_history_item_should_be_1", case_name),
        execute_query(qry.as_str(), fixture.ctx()).await,
        expected,
    )
    .await
}
