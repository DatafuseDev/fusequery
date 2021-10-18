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

use common_arrow::arrow::array::UInt16Array;
use common_arrow::arrow::array::UInt32Array;
use common_arrow::arrow::array::UInt64Array;
use common_arrow::arrow::array::UInt8Array;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;

use crate::scalars::dates::number_function::ToDayOfMonthFunction;
use crate::scalars::dates::number_function::ToDayOfWeekFunction;
use crate::scalars::dates::number_function::ToDayOfYearFunction;
use crate::scalars::dates::number_function::ToMonthFunction;
use crate::scalars::ToYYYYMMDDFunction;
use crate::scalars::ToYYYYMMDDhhmmssFunction;
use crate::scalars::ToYYYYMMFunction;

#[test]
fn test_toyyyymm_date16_function() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([197001; 1]);

        assert_eq!(actual, &expected);
    }

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![
        0u16, 0u16, 0u16, 0u16,
    ])]);

    {
        let toyyyymm = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];

        let result = toyyyymm.eval(&columns, block.num_rows())?;
        assert_eq!(result.data_type(), DataType::UInt32);
        assert_eq!(result.len(), 4);

        let actual_ref = result
            .to_array()?
            .u32()?
            .inner()
            .values()
            .as_slice()
            .to_vec();
        assert_eq!(vec![197001u32, 197001u32, 197001u32, 197001u32], actual_ref);
    }

    Ok(())
}

#[test]
fn test_toyyyymm_date32_function() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([197001; 1]);

        assert_eq!(actual, &expected);
    }

    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32, 1, 2, 3])]);

    {
        let toyyyymm = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];

        let result = toyyyymm.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 4);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result
            .to_array()?
            .u32()?
            .inner()
            .values()
            .as_slice()
            .to_vec();
        assert_eq!(vec![197001u32, 197001u32, 197001u32, 197001u32], actual_ref);
    }

    Ok(())
}

#[test]
fn test_toyyyymm_date_time_function() -> Result<()> {
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u32])]);

    {
        let col = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([197001; 1]);

        assert_eq!(actual, &expected);
    }

    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![
        0u32, 1u32, 2u32, 3u32,
    ])]);

    {
        let toyyyymm = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];

        let result = toyyyymm.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 4);

        let actual_ref = result
            .to_array()?
            .u32()?
            .inner()
            .values()
            .as_slice()
            .to_vec();
        assert_eq!(vec![197001u32, 197001u32, 197001u32, 197001u32], actual_ref);
    }

    Ok(())
}

#[test]
fn test_toyyyymm_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([197001; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([197001; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(0u32)),
        15,
    )]);
    {
        let col = ToYYYYMMFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([197001; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_toyyyymmdd_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToYYYYMMDDFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([19700101; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToYYYYMMDDFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([19700101; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-09-05 09:23:17 --- 1630833797
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1630833797u32])]);

    {
        let col = ToYYYYMMDDFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([20210905; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_toyyyymmdd_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToYYYYMMDDFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([19700101; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToYYYYMMDDFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([19700101; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-09-05 09:23:17 --- 1630833797
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1630833797u32)),
        15,
    )]);
    {
        let col = ToYYYYMMDDFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt32);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt32Array>().unwrap();
        let expected = UInt32Array::from_slice([20210905; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_toyyyymmddhhmmss_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-09-05 09:23:17 --- 1630833797
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1630833797u32])]);

    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([20210905092317; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_toyyyymmhhmmss_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([19700101000000; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-09-05 09:23:17 --- 1630833797
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1630833797u32)),
        15,
    )]);
    {
        let col = ToYYYYMMDDhhmmssFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt64);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt64Array>().unwrap();
        let expected = UInt64Array::from_slice([20210905092317; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tomonth_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-01 17:50:17 --- 1633081817
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633081817u32])]);

    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([10; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_tomonth_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-01 17:50:17 --- 1633081817
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633081817u32)),
        15,
    )]);
    {
        let col = ToMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([10; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofyear_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633173324u32])]);

    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([275; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofyear_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([1; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633173324u32)),
        15,
    )]);
    {
        let col = ToDayOfYearFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt16);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt16Array>().unwrap();
        let expected = UInt16Array::from_slice([275; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofweek_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633173324u32])]);

    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([6; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofweek_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([4; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633173324u32)),
        15,
    )]);
    {
        let col = ToDayOfWeekFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([6; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofmonth_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0u16])]);

    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);
        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![0i32])]);

    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 1]);

        assert_eq!(actual, &expected);
    }

    // dateTime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create_by_array(schema.clone(), vec![Series::new(vec![1633173324u32])]);

    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 1);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([2; 1]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}

#[test]
fn test_todayofmonth_constant_function() -> Result<()> {
    // date16
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date16, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt16(Some(0u16)),
        5,
    )]);
    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 5);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 5]);

        assert_eq!(actual, &expected);
    }

    // date32
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", DataType::Date32, false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::Int32(Some(0i32)),
        10,
    )]);
    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 10);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([1; 10]);

        assert_eq!(actual, &expected);
    }

    // datetime
    // 2021-10-02 19:15:24 --- 1633173324
    let schema =
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::DateTime32(None), false)]);
    let block = DataBlock::create(schema.clone(), vec![DataColumn::Constant(
        DataValue::UInt32(Some(1633173324u32)),
        15,
    )]);
    {
        let col = ToDayOfMonthFunction::try_create("a")?;
        let columns = vec![DataColumnWithField::new(
            block.try_column_by_name("a")?.clone(),
            schema.field_with_name("a")?.clone(),
        )];
        let result = col.eval(&columns, block.num_rows())?;
        assert_eq!(result.len(), 15);
        assert_eq!(result.data_type(), DataType::UInt8);

        let actual_ref = result.get_array_ref().unwrap();
        let actual = actual_ref.as_any().downcast_ref::<UInt8Array>().unwrap();
        let expected = UInt8Array::from_slice([2; 15]);

        assert_eq!(actual, &expected);
    }

    Ok(())
}
