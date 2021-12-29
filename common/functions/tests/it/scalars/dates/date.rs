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

use common_datavalues::prelude::*;
use common_exception::Result;
use common_functions::scalars::*;

use crate::scalars::scalar_function_test::test_scalar_functions_with_type;
use crate::scalars::scalar_function_test::ScalarFunctionTestWithType;

#[test]
fn test_round_function() -> Result<()> {
    let mut tests = vec![];

    for r in &[1, 60, 60 * 10, 60 * 15, 60 * 30, 60 * 60, 60 * 60 * 24] {
        tests.push((
            RoundFunction::try_create("toStartOfCustom", *r)?,
            ScalarFunctionTestWithType {
                name: "test-timeSlot-now",
                nullable: false,
                columns: vec![DataColumnWithField::new(
                    Series::new(vec![1630812366u32, 1630839682u32]).into(),
                    DataField::new("dummy_1", DataType::DateTime32(None), false),
                )],
                expect: Series::new(vec![1630812366u32 / r * r, 1630839682u32 / r * r]).into(),
                error: "",
            },
        ));
    }

    for (test_function, test) in tests {
        test_scalar_functions_with_type(test_function, &[test])?;
    }

    Ok(())
}

#[test]
fn test_to_start_of_function() -> Result<()> {
    let test = vec![ScalarFunctionTestWithType {
        name: "test-timeSlot-now",
        nullable: false,
        columns: vec![DataColumnWithField::new(
            Series::new(vec![1631705259u32]).into(),
            DataField::new("dummy_1", DataType::DateTime32(None), false),
        )],
        expect: Series::new(vec![18809u16]).into(),
        error: "",
    }];

    test_scalar_functions_with_type(
        ToStartOfQuarterFunction::try_create("toStartOfWeek")?,
        &test,
    )
}
