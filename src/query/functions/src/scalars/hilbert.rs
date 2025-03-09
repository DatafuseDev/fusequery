// Copyright 2021 Datafuse Labs
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

use std::sync::Arc;

use databend_common_expression::hilbert_index;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::ALL_NUMERICS_TYPES;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Column;
use databend_common_expression::FixedLengthEncoding;
use databend_common_expression::Function;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionEval;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::FunctionSignature;
use databend_common_expression::ScalarRef;
use databend_common_expression::Value;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_function_factory("hilbert_range_index", |_, args_type| {
        let args_num = args_type.len();
        if ![4, 6, 8, 10].contains(&args_num) {
            return None;
        }
        let sig_args_type = (0..args_num / 2)
            .flat_map(|idx| {
                [
                    DataType::Generic(idx),
                    DataType::Array(Box::new(DataType::Generic(idx))),
                ]
            })
            .collect();
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "hilbert_range_index".to_string(),
                args_type: sig_args_type,
                return_type: DataType::Binary,
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, _| FunctionDomain::Full),
                eval: Box::new(move |args, ctx| {
                    let input_all_scalars = args.iter().all(|arg| arg.as_scalar().is_some());
                    let process_rows = if input_all_scalars { 1 } else { ctx.num_rows };
                    let mut builder = BinaryType::create_builder(process_rows, &[]);
                    for index in 0..process_rows {
                        let mut points = Vec::with_capacity(args_num / 2);
                        for i in (0..args_num).step_by(2) {
                            let arg1 = &args[i];
                            let arg2 = &args[i + 1];

                            let val = unsafe { arg1.index_unchecked(index) };
                            let arr = unsafe { arg2.index_unchecked(index) };
                            let arr = arr.as_array().unwrap();
                            let id = calc_range_partition_id(val, arr).min(65535) as u16;
                            let key = id.encode();
                            points.push(key);
                        }
                        let points = points
                            .iter()
                            .map(|array| array.as_slice())
                            .collect::<Vec<_>>();
                        let slice = hilbert_index(&points, 2);
                        builder.put_slice(&slice);
                        builder.commit_row();
                    }

                    if input_all_scalars {
                        Value::Scalar(BinaryType::upcast_scalar(BinaryType::build_scalar(builder)))
                    } else {
                        Value::Column(BinaryType::upcast_column(BinaryType::build_column(builder)))
                    }
                }),
            },
        }))
    });

    registry.register_passthrough_nullable_1_arg::<StringType, BinaryType, _, _>(
        "hilbert_key",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, BinaryType>(|val, builder, _| {
            let bytes = val.as_bytes();
            builder.put_slice(bytes);
            builder.commit_row();
        }),
    );

    for ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_passthrough_nullable_1_arg::<NumberType<NUM_TYPE>, BinaryType, _, _>(
                        "hilbert_key",
                        |_, _| FunctionDomain::Full,
                        vectorize_with_builder_1_arg::<NumberType<NUM_TYPE>, BinaryType>(
                            |val, builder, _| {
                                let encoded = val.encode();
                                builder.put_slice(&encoded);
                                builder.commit_row();
                            },
                        ),
                    );
            }
        })
    }

    registry.register_combine_nullable_2_arg::<ArrayType<NullableType<BinaryType>>, NumberType<u64>, BinaryType, _, _>(
        "hilbert_index",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<ArrayType<NullableType<BinaryType>>, NumberType<u64>, NullableType<BinaryType>>(
            |val, len, builder, ctx| {
                let mut points = Vec::with_capacity(val.len());
                for a in val.iter() {
                    if a.is_none() {
                        builder.push_null();
                        return;
                    }
                    points.push(a.unwrap());
                }
                let dimension = points.len();

                if std::intrinsics::unlikely(len > 64) {
                    ctx.set_error(builder.len(), "Width must be less than or equal to 64");
                    builder.push_null();
                } else if std::intrinsics::unlikely(!(2..=5).contains(&dimension)) {
                    ctx.set_error(builder.len(), "Dimension must between 2 and 5");
                    builder.push_null();
                } else {
                    let slice = hilbert_index(&points, len as usize);
                    builder.push(&slice);
                }
            },
        ),
    );

    // This `range_partition_id(col, range_bounds)` function calculates the partition ID for each value
    // in the column based on the specified partition boundaries.
    // The column values are conceptually divided into multiple partitions defined by the range_bounds.
    // For example, given the column values (0, 1, 3, 6, 8) and a partition configuration with 3 partitions,
    // the range_bounds might be [1, 6]. The function would then return partition IDs as (0, 0, 1, 1, 2).
    registry
        .register_2_arg_core::<GenericType<0>, ArrayType<GenericType<0>>, NumberType<u64>, _, _>(
            "range_partition_id",
            |_, _, _| FunctionDomain::Full,
            vectorize_with_builder_2_arg::<
                GenericType<0>,
                ArrayType<GenericType<0>>,
                NumberType<u64>,
            >(|val, arr, builder, _| {
                let id = calc_range_partition_id(val, &arr);
                builder.push(id);
            }),
        );

    registry.register_2_arg_core::<NullableType<GenericType<0>>, NullableType<ArrayType<GenericType<0>>>, NumberType<u64>, _, _>(
        "range_partition_id",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<NullableType<GenericType<0>>, NullableType<ArrayType<GenericType<0>>>, NumberType<u64>>(|val, arr, builder, _| {
            let id = match (val, arr) {
                (Some(val), Some(arr)) => calc_range_partition_id(val, &arr),
                (None, Some(arr)) => arr.len() as u64,
                _ => 0,
            };
            builder.push(id);
        }),
    );
}

fn calc_range_partition_id(val: ScalarRef, arr: &Column) -> u64 {
    let mut low = 0;
    let mut high = arr.len();
    while low < high {
        let mid = low + ((high - low) / 2);
        let bound = unsafe { arr.index_unchecked(mid) };
        if val > bound {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    low as u64
}
