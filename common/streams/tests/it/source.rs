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

use std::fs::File;
use std::io::Write;

use common_base::tokio;
use common_dal2::readers::SeekableReader;
use common_dal2::services::fs;
use common_dal2::Operator;
use common_datablocks::assert_blocks_eq;
use common_datablocks::DataBlock;
use common_datavalues::prelude::DataColumn;
use common_datavalues::prelude::Series;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_streams::CsvSource;
use common_streams::ParquetSource;
use common_streams::Source;
use common_streams::ValueSource;
use futures::io::BufReader;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_values() {
    let buffer =
        "(1,  'str',   1) , (-1, ' str ' ,  1.1) , ( 2,  'aa aa', 2.2),  (3, \"33'33\", 3.3)   ";

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
        DataField::new("c", DataType::Float64, false),
    ]);
    let mut values_source = ValueSource::new(buffer.as_bytes(), schema, 10);
    let block = values_source.read().await.unwrap().unwrap();
    assert_blocks_eq(
        vec![
            "+----+-------+-----+",
            "| a  | b     | c   |",
            "+----+-------+-----+",
            "| 1  | str   | 1   |",
            "| -1 |  str  | 1.1 |",
            "| 2  | aa aa | 2.2 |",
            "| 3  | 33'33 | 3.3 |",
            "+----+-------+-----+",
        ],
        &[block],
    );

    let block = values_source.read().await.unwrap();
    assert!(block.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_csvs() {
    for field_delimitor in [b',', b'\t', b'#'] {
        for record_delimitor in [b'\n', b'\r', b'~'] {
            let dir = tempfile::tempdir().unwrap();
            let name = "my-temporary-note.txt";
            let file_path = dir.path().join(name);
            let mut file = File::create(file_path).unwrap();

            write!(
                file,
                "1{}\"1\"{}1.11{}2{}\"2\"{}2{}3{}\"3-'3'-3\"{}3\"{}",
                field_delimitor as char,
                field_delimitor as char,
                record_delimitor as char,
                field_delimitor as char,
                field_delimitor as char,
                record_delimitor as char,
                field_delimitor as char,
                field_delimitor as char,
                record_delimitor as char,
            )
            .unwrap();

            let schema = DataSchemaRefExt::create(vec![
                DataField::new("a", DataType::Int8, false),
                DataField::new("b", DataType::String, false),
                DataField::new("c", DataType::Float64, false),
            ]);

            let local = Operator::new(
                fs::Backend::build()
                    .root(dir.path().to_str().unwrap())
                    .finish(),
            );
            let stream = local.read(name).run().await.unwrap();
            let mut csv_source =
                CsvSource::try_create(stream, schema, false, field_delimitor, record_delimitor, 10)
                    .unwrap();
            let block = csv_source.read().await.unwrap().unwrap();
            assert_blocks_eq(
                vec![
                    "+---+---------+------+",
                    "| a | b       | c    |",
                    "+---+---------+------+",
                    "| 1 | 1       | 1.11 |",
                    "| 2 | 2       | 2    |",
                    "| 3 | 3-'3'-3 | 3    |",
                    "+---+---------+------+",
                ],
                &[block],
            );

            let block = csv_source.read().await.unwrap();
            assert!(block.is_none());

            drop(file);
            dir.close().unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_source_parquet() -> Result<()> {
    use common_datavalues::DataType;
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
    ]);

    let arrow_schema = schema.to_arrow();

    use common_arrow::arrow::io::parquet::write::*;
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Lz4, // let's begin with lz4
        version: Version::V2,
    };

    use common_datavalues::prelude::SeriesFrom;
    let col_a = Series::new(vec![1i8, 1, 2, 1, 2, 3]);
    let col_b = Series::new(vec!["1", "1", "2", "1", "2", "3"]);
    let sample_block = DataBlock::create(schema.clone(), vec![
        DataColumn::Array(col_a),
        DataColumn::Array(col_b),
    ]);

    use common_arrow::arrow::record_batch::RecordBatch;
    let batch = RecordBatch::try_from(sample_block)?;
    use common_arrow::parquet::encoding::Encoding;
    let encodings = std::iter::repeat(Encoding::Plain)
        .take(arrow_schema.fields.len())
        .collect::<Vec<_>>();

    let page_nums_expects = 3;
    let name = "test-parquet";
    let dir = tempfile::tempdir().unwrap();

    // write test parquet
    let len = {
        let rg_iter = std::iter::repeat(batch).map(Ok).take(page_nums_expects);
        let row_groups = RowGroupIterator::try_new(rg_iter, &arrow_schema, options, encodings)?;
        let parquet_schema = row_groups.parquet_schema().clone();
        let path = dir.path().join(name);
        let mut writer = File::create(path).unwrap();
        common_arrow::parquet::write::write_file(
            &mut writer,
            row_groups,
            parquet_schema,
            options,
            None,
            None,
        )
        .map_err(|e| ErrorCode::ParquetError(e.to_string()))?
    };

    let local = Operator::new(
        fs::Backend::build()
            .root(dir.path().to_str().unwrap())
            .finish(),
    );
    let stream = SeekableReader::new(local, name, len);
    let stream = BufReader::with_capacity(4 * 1024 * 1024, stream);

    let default_proj = (0..schema.fields().len())
        .into_iter()
        .collect::<Vec<usize>>();

    let mut page_nums = 0;
    let mut parquet_source = ParquetSource::new(stream, schema, default_proj);
    // expects `page_nums_expects` blocks, and
    while let Some(block) = parquet_source.read().await? {
        page_nums += 1;
        // for each block, the content is the same of `sample_block`
        assert_blocks_eq(
            vec![
                "+---+---+",
                "| a | b |",
                "+---+---+",
                "| 1 | 1 |",
                "| 1 | 1 |",
                "| 2 | 2 |",
                "| 1 | 1 |",
                "| 2 | 2 |",
                "| 3 | 3 |",
                "+---+---+",
            ],
            &[block],
        );
    }

    assert_eq!(page_nums_expects, page_nums);
    Ok(())
}
