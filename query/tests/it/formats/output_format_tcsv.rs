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

use common_arrow::arrow::bitmap::MutableBitmap;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use databend_query::formats::output_format::OutputFormatType;
use pretty_assertions::assert_eq;

fn test_data_block(is_nullable: bool) -> Result<()> {
    let schema = match is_nullable {
        false => DataSchemaRefExt::create(vec![
            DataField::new("c1", i32::to_data_type()),
            DataField::new("c2", Vu8::to_data_type()),
            DataField::new("c3", bool::to_data_type()),
            DataField::new("c4", f64::to_data_type()),
            DataField::new("c5", DateType::new_impl()),
        ]),
        true => DataSchemaRefExt::create(vec![
            DataField::new_nullable("c1", i32::to_data_type()),
            DataField::new_nullable("c2", Vu8::to_data_type()),
            DataField::new_nullable("c3", bool::to_data_type()),
            DataField::new_nullable("c4", f64::to_data_type()),
            DataField::new_nullable("c5", DateType::new_impl()),
        ]),
    };

    let block = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![1, 2, 3]),
        Series::from_data(vec!["a", "b", "c"]),
        Series::from_data(vec![true, true, false]),
        Series::from_data(vec![1.1, 2.2, 3.3]),
        Series::from_data(vec![1_i32, 2_i32, 3_i32]),
    ]);

    let block = if is_nullable {
        let columns = block
            .columns()
            .iter()
            .map(|c| {
                let mut validity = MutableBitmap::new();
                validity.extend_constant(c.len(), true);
                NullableColumn::wrap_inner(c.clone(), Some(validity.into()))
            })
            .collect();
        DataBlock::create(schema.clone(), columns)
    } else {
        block
    };

    let mut format_setting = FormatSettings::default();

    {
        let fmt = OutputFormatType::TSV;
        let mut formatter = fmt.create_format(schema.clone());
        let buffer = formatter.serialize_block(&block, &format_setting)?;

        let tsv_block = String::from_utf8(buffer)?;
        let expect = "1\ta\t1\t1.1\t1970-01-02\n\
                            2\tb\t1\t2.2\t1970-01-03\n\
                            3\tc\t0\t3.3\t1970-01-04\n";
        assert_eq!(&tsv_block, expect);

        let fmt = OutputFormatType::TSVWithNames;
        let formatter = fmt.create_format(schema.clone());
        let buffer = formatter.serialize_prefix(&format_setting)?;
        let tsv_block = String::from_utf8(buffer)?;
        let names = "c1\tc2\tc3\tc4\tc5\n".to_string();
        assert_eq!(tsv_block, names);

        let fmt = OutputFormatType::TSVWithNamesAndTypes;
        let formatter = fmt.create_format(schema.clone());
        let buffer = formatter.serialize_prefix(&format_setting)?;
        let tsv_block = String::from_utf8(buffer)?;

        let types = if is_nullable {
            "Nullable(Int32)\tNullable(String)\tNullable(Boolean)\tNullable(Float64)\tNullable(Date)\n"
                .to_string()
        } else {
            "Int32\tString\tBoolean\tFloat64\tDate\n".to_string()
        };
        assert_eq!(tsv_block, names + &types);
    }

    {
        format_setting.record_delimiter = vec![b'%'];
        format_setting.field_delimiter = vec![b'$'];

        let fmt = OutputFormatType::CSV;
        let mut formatter = fmt.create_format(schema);
        let buffer = formatter.serialize_block(&block, &format_setting)?;

        let csv_block = String::from_utf8(buffer)?;
        let expect = "1$\"a\"$1$1.1$\"1970-01-02\"%\
                            2$\"b\"$1$2.2$\"1970-01-03\"%\
                            3$\"c\"$0$3.3$\"1970-01-04\"%";
        assert_eq!(&csv_block, expect);
    }
    Ok(())
}

#[test]
fn test_data_block_nullable() -> Result<()> {
    test_data_block(true)
}

#[test]
fn test_data_block_not_nullable() -> Result<()> {
    test_data_block(false)
}
