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

use common_arrow::arrow::array::ArrayRef;
use common_exception::Result;

use crate::arrays::builders::*;
use crate::arrays::get_list_builder;
use crate::arrays::ops::scatter::ArrayScatter;
use crate::prelude::*;

#[test]
fn test_scatter() -> Result<()> {
    // Test DFUint16Array
    let df_uint16_array = DFUInt16Array::new_from_iter(1u16..11u16);
    // Create the indice array
    let indices = vec![1, 2, 3, 1, 3, 2, 0, 3, 1, 0];
    // The number of rows should be equal to the length of indices
    assert_eq!(df_uint16_array.len(), indices.len());

    let array_vec = unsafe { df_uint16_array.scatter_unchecked(&mut indices.into_iter(), 4)? };
    assert_eq!(&[7u16, 10], array_vec[0].as_ref().values().as_slice());
    assert_eq!(&[1u16, 4, 9], array_vec[1].as_ref().values().as_slice());
    assert_eq!(&[2u16, 6], array_vec[2].as_ref().values().as_slice());
    assert_eq!(&[3u16, 5, 8], array_vec[3].as_ref().values().as_slice());

    // Test DFUint16Array
    let df_utf8_array = DFUtf8Array::new_from_slice(&["a", "b", "c", "d"]);
    let indices = vec![1, 0, 1, 1];
    assert_eq!(df_utf8_array.len(), indices.len());

    let array_vec = unsafe { df_utf8_array.scatter_unchecked(&mut indices.into_iter(), 2)? };
    let v1: Vec<&str> = array_vec[0].into_no_null_iter().collect();
    let v2: Vec<&str> = array_vec[1].into_no_null_iter().collect();
    assert_eq!(vec!["b"], v1);
    assert_eq!(vec!["a", "c", "d"], v2);

    // Test BooleanArray
    let df_bool_array = DFBooleanArray::new_from_slice(&[true, false, true, false]);
    let indices = vec![1, 0, 0, 1];
    assert_eq!(df_bool_array.len(), indices.len());

    let array_vec = unsafe { df_bool_array.scatter_unchecked(&mut indices.into_iter(), 2)? };
    assert_eq!(
        vec![false, true],
        array_vec[0].into_no_null_iter().collect::<Vec<bool>>()
    );
    assert_eq!(
        vec![true, false],
        array_vec[1].into_no_null_iter().collect::<Vec<bool>>()
    );

    // Test BinaryArray
    let mut binary_builder = BinaryArrayBuilder::with_capacity(8);
    binary_builder.append_value(&"12");
    binary_builder.append_value(&"ab");
    binary_builder.append_value(&"c1");
    binary_builder.append_value(&"32");
    let df_binary_array = binary_builder.finish();
    let indices = vec![1, 0, 0, 1];
    let array_vec = unsafe { df_binary_array.scatter_unchecked(&mut indices.into_iter(), 2)? };

    let values: Vec<Vec<u8>> = (0..array_vec[0].len())
        .map(|idx| array_vec[0].downcast_ref().value(idx).to_vec())
        .collect();

    assert_eq!(vec![b"ab".to_vec(), b"c1".to_vec()], values);

    let values: Vec<Vec<u8>> = (0..array_vec[1].len())
        .map(|idx| array_vec[1].downcast_ref().value(idx).to_vec())
        .collect();
    assert_eq!(vec![b"12".to_vec(), b"32".to_vec()], values);

    // Test LargeListArray
    let mut builder = get_list_builder(&DataType::UInt16, 12, 3);
    builder.append_series(&Series::new(vec![1_u16, 2, 3]));
    builder.append_series(&Series::new(vec![7_u16, 8, 9]));
    builder.append_series(&Series::new(vec![10_u16, 11, 12]));
    builder.append_series(&Series::new(vec![4_u16, 5, 6]));
    let df_list = builder.finish();

    let indices = vec![1, 0, 0, 1];
    let array_vec = unsafe { df_list.scatter_unchecked(&mut indices.into_iter(), 2)? };

    let c0: ArrayRef = Arc::from(array_vec[0].downcast_ref().value(0));
    let c0 = DFUInt16Array::from(c0);
    let c1: ArrayRef = Arc::from(array_vec[1].downcast_ref().value(0));
    let c1 = DFUInt16Array::from(c1);

    assert_eq!(&[7, 8, 9], c0.downcast_ref().values().as_slice());
    assert_eq!(&[1, 2, 3], c1.downcast_ref().values().as_slice());

    let c0: ArrayRef = Arc::from(array_vec[0].downcast_ref().value(1));
    let c0 = DFUInt16Array::from(c0);
    let c1: ArrayRef = Arc::from(array_vec[1].downcast_ref().value(1));
    let c1 = DFUInt16Array::from(c1);

    assert_eq!(&[10, 11, 12], c0.downcast_ref().values().as_slice());
    assert_eq!(&[4, 5, 6], c1.downcast_ref().values().as_slice());

    Ok(())
}
