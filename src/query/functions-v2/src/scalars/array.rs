// Copyright 2022 Datafuse Labs.
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

use common_expression::types::array::ArrayColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::EmptyArrayType;
use common_expression::types::GenericType;
use common_expression::Column;
use common_expression::Domain;
use common_expression::Function;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::Value;
use common_expression::ValueRef;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_0_arg_core::<EmptyArrayType, _, _>(
        "array",
        FunctionProperty::default(),
        || Some(()),
        |_| Ok(Value::Scalar(())),
    );

    registry.register_function_factory("array", |_, args_type| {
        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "array",
                args_type: vec![DataType::Generic(0); args_type.len()],
                return_type: DataType::Array(Box::new(DataType::Generic(0))),
                property: FunctionProperty::default(),
            },
            calc_domain: Box::new(|args_domain| {
                Some(args_domain.iter().fold(Domain::Array(None), |acc, x| {
                    acc.merge(&Domain::Array(Some(Box::new(x.clone()))))
                }))
            }),
            eval: Box::new(|args, ctx| {
                let len = args.iter().find_map(|arg| match arg {
                    ValueRef::Column(col) => Some(col.len()),
                    _ => None,
                });

                let mut builder: ArrayColumnBuilder<GenericType<0>> =
                    ArrayColumnBuilder::with_capacity(len.unwrap_or(1), 0, ctx.generics);

                for idx in 0..(len.unwrap_or(1)) {
                    for arg in args {
                        match arg {
                            ValueRef::Scalar(scalar) => {
                                builder.put_item(scalar.clone());
                            }
                            ValueRef::Column(col) => unsafe {
                                builder.put_item(col.index_unchecked(idx));
                            },
                        }
                    }
                    builder.commit_row();
                }

                match len {
                    Some(_) => Ok(Value::Column(Column::Array(Box::new(
                        builder.build().upcast(),
                    )))),
                    None => Ok(Value::Scalar(Scalar::Array(builder.build_scalar()))),
                }
            }),
        }))
    });
}
