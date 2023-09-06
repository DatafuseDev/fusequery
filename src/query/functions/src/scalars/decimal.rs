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

use std::cmp::Ord;
use std::ops::*;
use std::sync::Arc;

use common_arrow::arrow::buffer::Buffer;
use common_expression::serialize::read_decimal_with_size;
use common_expression::type_check::common_super_type;
use common_expression::types::decimal::*;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::string::StringColumn;
use common_expression::types::string::StringDomain;
use common_expression::types::*;
use common_expression::with_integer_mapped_type;
use common_expression::wrap_nullable;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Domain;
use common_expression::EvalContext;
use common_expression::Function;
use common_expression::FunctionDomain;
use common_expression::FunctionEval;
use common_expression::FunctionRegistry;
use common_expression::FunctionSignature;
use common_expression::Scalar;
use common_expression::ScalarRef;
use common_expression::SimpleDomainCmp;
use common_expression::Value;
use common_expression::ValueRef;
use ethnum::i256;
use num_traits::AsPrimitive;
use ordered_float::OrderedFloat;

macro_rules! op_decimal {
    ($a: expr, $b: expr, $ctx: expr, $common_type: expr, $op: ident, $scale_a: expr, $scale_b: expr, $is_divide: expr) => {
        match $common_type {
            DecimalDataType::Decimal128(size) => {
                binary_decimal!(
                    $a, $b, $ctx, $op, size, $scale_a, $scale_b, i128, Decimal128, $is_divide
                )
            }
            DecimalDataType::Decimal256(size) => {
                binary_decimal!(
                    $a, $b, $ctx, $op, size, $scale_a, $scale_b, i256, Decimal256, $is_divide
                )
            }
        }
    };
    ($a: expr, $b: expr, $return_type: expr, $op: ident) => {
        match $return_type {
            DecimalDataType::Decimal128(_) => {
                compare_decimal!($a, $b, $op, Decimal128)
            }
            DecimalDataType::Decimal256(_) => {
                compare_decimal!($a, $b, $op, Decimal256)
            }
        }
    };
}

macro_rules! compare_decimal {
    ($a: expr, $b: expr, $op: ident, $decimal_type: tt) => {{
        match ($a, $b) {
            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_b, _))),
            ) => {
                let result = buffer_a
                    .iter()
                    .zip(buffer_b.iter())
                    .map(|(a, b)| a.cmp(b).$op())
                    .collect();

                Value::Column(Column::Boolean(result))
            }

            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => {
                let result = buffer.iter().map(|a| a.cmp(b).$op()).collect();

                Value::Column(Column::Boolean(result))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
            ) => {
                let result = buffer.iter().map(|b| a.cmp(b).$op()).collect();

                Value::Column(Column::Boolean(result))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => Value::Scalar(Scalar::Boolean(a.cmp(b).$op())),

            _ => unreachable!("arg type of cmp op is not required decimal"),
        }
    }};
}

macro_rules! binary_decimal {
    ($a: expr, $b: expr, $ctx: expr, $op: ident, $size: expr, $scale_a: expr, $scale_b: expr, $type_name: ty, $decimal_type: tt, $is_divide: expr) => {{
        let scale_a = <$type_name>::e($scale_a);
        let scale_b = <$type_name>::e($scale_b);

        let zero = <$type_name>::zero();
        let one = <$type_name>::one();
        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        match ($a, $b) {
            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer_b, _))),
            ) => {
                let mut result = Vec::with_capacity(buffer_a.len());

                for (a, b) in buffer_a.iter().zip(buffer_b.iter()) {
                    if $is_divide && std::intrinsics::unlikely(*b == zero) {
                        $ctx.set_error(result.len(), "divided by zero");
                        result.push(one);
                    } else {
                        let t = (a * scale_a).$op(b) / scale_b;
                        if t < min_for_precision || t > max_for_precision {
                            $ctx.set_error(
                                result.len(),
                                concat!("Decimal overflow at line : ", line!()),
                            );
                            result.push(one);
                        } else {
                            result.push(t);
                        }
                    }
                }
                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(
                    result.into(),
                    $size,
                )))
            }

            (
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => {
                let mut result = Vec::with_capacity(buffer.len());

                if $is_divide && std::intrinsics::unlikely(*b == zero) {
                    $ctx.set_error(result.len(), "divided by zero");
                    result.push(one);
                } else {
                    for a in buffer.iter() {
                        let t = (a * scale_a).$op(b) / scale_b;
                        if t < min_for_precision || t > max_for_precision {
                            $ctx.set_error(
                                result.len(),
                                concat!("Decimal overflow at line : ", line!()),
                            );
                            result.push(one);
                        } else {
                            result.push(t);
                        }
                    }
                }

                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(
                    result.into(),
                    $size,
                )))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Column(Column::Decimal(DecimalColumn::$decimal_type(buffer, _))),
            ) => {
                let mut result = Vec::with_capacity(buffer.len());

                for b in buffer.iter() {
                    if $is_divide && std::intrinsics::unlikely(*b == zero) {
                        $ctx.set_error(result.len(), "divided by zero");
                        result.push(one);
                    } else {
                        let t = (a * scale_a).$op(b) / scale_b;
                        if t < min_for_precision || t > max_for_precision {
                            $ctx.set_error(
                                result.len(),
                                concat!("Decimal overflow at line : ", line!()),
                            );
                            result.push(one);
                        } else {
                            result.push(t);
                        }
                    }
                }
                Value::Column(Column::Decimal(DecimalColumn::$decimal_type(
                    result.into(),
                    $size,
                )))
            }

            (
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(a, _))),
                ValueRef::Scalar(ScalarRef::Decimal(DecimalScalar::$decimal_type(b, _))),
            ) => {
                let mut t = zero;
                if $is_divide && std::intrinsics::unlikely(*b == zero) {
                    $ctx.set_error(0, "divided by zero");
                } else {
                    t = (a * scale_a).$op(b) / scale_b;
                    if t < min_for_precision || t > max_for_precision {
                        $ctx.set_error(0, concat!("Decimal overflow at line : ", line!()));
                    }
                }
                Value::Scalar(Scalar::Decimal(DecimalScalar::$decimal_type(t, $size)))
            }

            _ => unreachable!("arg type of binary op is not required decimal"),
        }
    }};
}

macro_rules! register_decimal_compare_op {
    ($registry: expr, $name: expr, $op: ident, $domain_op: tt) => {
        $registry.register_function_factory($name, |_, args_type| {
            if args_type.len() != 2 {
                return None;
            }

            let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
            let args_type: Vec<DataType> = args_type.iter().map(|x| x.remove_nullable()).collect();

            // Only works for one of is decimal types
            if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
                return None;
            }

            let common_type = common_super_type(args_type[0].clone(), args_type[1].clone(), &[])?;

            if !common_type.is_decimal() {
                return None;
            }

            // Comparison between different decimal types must be same siganature types
            Some(Arc::new(if has_nullable {
                // Cannot use `function.wrap_nullable` because this method will erase `calc_domain`.
                Function {
                    signature: FunctionSignature {
                        name: $name.to_string(),
                        args_type: vec![common_type.wrap_nullable(), common_type.wrap_nullable()],
                        return_type: DataType::Nullable(Box::new(DataType::Boolean)),
                    },
                    eval: FunctionEval::Scalar {
                        calc_domain: Box::new(|_, d| match (&d[0], &d[1]) {
                            (Domain::Nullable(d1), Domain::Nullable(d2))
                                if d1.value.is_some() && d2.value.is_some() =>
                            {
                                let new_domain = match (
                                    d1.value.as_ref().unwrap().as_ref(),
                                    d2.value.as_ref().unwrap().as_ref(),
                                ) {
                                    (
                                        Domain::Decimal(DecimalDomain::Decimal128(d1, _)),
                                        Domain::Decimal(DecimalDomain::Decimal128(d2, _)),
                                    ) => d1.$domain_op(d2),
                                    (
                                        Domain::Decimal(DecimalDomain::Decimal256(d1, _)),
                                        Domain::Decimal(DecimalDomain::Decimal256(d2, _)),
                                    ) => d1.$domain_op(d2),
                                    _ => {
                                        unreachable!("Expect two same decimal domains, got {:?}", d)
                                    }
                                };
                                new_domain.map(|d| {
                                    Domain::Nullable(NullableDomain {
                                        has_null: d1.has_null || d2.has_null,
                                        value: Some(Box::new(Domain::Boolean(d))),
                                    })
                                })
                            }
                            (_, _) => FunctionDomain::Full,
                        }),
                        eval: Box::new(wrap_nullable(move |args, _ctx| {
                            op_decimal!(&args[0], &args[1], common_type.as_decimal().unwrap(), $op)
                        })),
                    },
                }
            } else {
                Function {
                    signature: FunctionSignature {
                        name: $name.to_string(),
                        args_type: vec![common_type.clone(), common_type.clone()],
                        return_type: DataType::Boolean,
                    },
                    eval: FunctionEval::Scalar {
                        calc_domain: Box::new(|_, d| {
                            let new_domain = match (&d[0], &d[1]) {
                                (
                                    Domain::Decimal(DecimalDomain::Decimal128(d1, _)),
                                    Domain::Decimal(DecimalDomain::Decimal128(d2, _)),
                                ) => d1.$domain_op(d2),
                                (
                                    Domain::Decimal(DecimalDomain::Decimal256(d1, _)),
                                    Domain::Decimal(DecimalDomain::Decimal256(d2, _)),
                                ) => d1.$domain_op(d2),
                                _ => unreachable!("Expect two same decimal domains, got {:?}", d),
                            };
                            new_domain.map(|d| Domain::Boolean(d))
                        }),
                        eval: Box::new(move |args, _ctx| {
                            op_decimal!(&args[0], &args[1], common_type.as_decimal().unwrap(), $op)
                        }),
                    },
                }
            }))
        });
    };
}

fn domain_plus<T: Decimal>(
    lhs: &SimpleDomain<T>,
    rhs: &SimpleDomain<T>,
    _lscale: u32,
    _rscale: u32,
    precision: u8,
) -> Option<SimpleDomain<T>> {
    // For plus, the scale of the two operands must be the same.
    let min = T::min_for_precision(precision);
    let max = T::max_for_precision(precision);
    Some(SimpleDomain {
        min: lhs
            .min
            .checked_add(rhs.min)
            .filter(|&m| m >= min && m <= max)?,
        max: lhs
            .max
            .checked_add(rhs.max)
            .filter(|&m| m >= min && m <= max)?,
    })
}

fn domain_minus<T: Decimal>(
    lhs: &SimpleDomain<T>,
    rhs: &SimpleDomain<T>,
    _lscale: u32,
    _rscale: u32,
    precision: u8,
) -> Option<SimpleDomain<T>> {
    // For minus, the scale of the two operands must be the same.
    let min = T::min_for_precision(precision);
    let max = T::max_for_precision(precision);
    Some(SimpleDomain {
        min: lhs
            .min
            .checked_sub(rhs.max)
            .filter(|&m| m >= min && m <= max)?,
        max: lhs
            .max
            .checked_sub(rhs.min)
            .filter(|&m| m >= min && m <= max)?,
    })
}

fn domain_mul<T: Decimal>(
    lhs: &SimpleDomain<T>,
    rhs: &SimpleDomain<T>,
    lscale: u32,
    rscale: u32,
    precision: u8,
) -> Option<SimpleDomain<T>> {
    // For mul, the scale of the two operands are not the same.
    let lscale = T::e(lscale);
    let rscale = T::e(rscale);
    let min = T::min_for_precision(precision);
    let max = T::max_for_precision(precision);

    let a = lhs
        .min
        .checked_mul(lscale)?
        .checked_mul(rhs.min)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;
    let b = lhs
        .min
        .checked_mul(lscale)?
        .checked_mul(rhs.max)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;
    let c = lhs
        .max
        .checked_mul(lscale)?
        .checked_mul(rhs.min)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;
    let d = lhs
        .max
        .checked_mul(lscale)?
        .checked_mul(rhs.max)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;

    Some(SimpleDomain {
        min: a.min(b).min(c).min(d),
        max: a.max(b).max(c).max(d),
    })
}

fn domain_div<T: Decimal>(
    lhs: &SimpleDomain<T>,
    rhs: &SimpleDomain<T>,
    lscale: u32,
    rscale: u32,
    precision: u8,
) -> Option<SimpleDomain<T>> {
    // For div, the scale of the two operands are not the same.
    let lscale = T::e(lscale);
    let rscale = T::e(rscale);
    let min = T::min_for_precision(precision);
    let max = T::max_for_precision(precision);

    let a = lhs
        .min
        .checked_mul(lscale)?
        .checked_div(rhs.min)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;
    let b = lhs
        .min
        .checked_mul(lscale)?
        .checked_div(rhs.max)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;
    let c = lhs
        .max
        .checked_mul(lscale)?
        .checked_div(rhs.min)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;
    let d = lhs
        .max
        .checked_mul(lscale)?
        .checked_div(rhs.max)?
        .checked_div(rscale)
        .filter(|&m| m >= min && m <= max)?;

    Some(SimpleDomain {
        min: a.min(b).min(c).min(d),
        max: a.max(b).max(c).max(d),
    })
}

macro_rules! register_decimal_binary_op {
    ($registry: expr, $name: expr, $op: ident, $domain_op: ident) => {
        $registry.register_function_factory($name, |_, args_type| {
            if args_type.len() != 2 {
                return None;
            }

            let has_nullable = args_type.iter().any(|x| x.is_nullable_or_null());
            let args_type: Vec<DataType> = args_type.iter().map(|x| x.remove_nullable()).collect();

            // number X decimal -> decimal
            // decimal X number -> decimal
            // decimal X decimal -> decimal
            if !args_type[0].is_decimal() && !args_type[1].is_decimal() {
                return None;
            }

            let decimal_a =
                DecimalDataType::from_size(args_type[0].get_decimal_properties()?).unwrap();
            let decimal_b =
                DecimalDataType::from_size(args_type[1].get_decimal_properties()?).unwrap();

            let is_multiply = $name == "multiply";
            let is_divide = $name == "divide";
            let is_plus_minus = !is_multiply && !is_divide;

            let return_decimal_type = DecimalDataType::binary_result_type(
                &decimal_a,
                &decimal_b,
                is_multiply,
                is_divide,
                is_plus_minus,
            )
            .ok()?;

            let common_decimal_type = if is_divide {
                DecimalDataType::div_common_type(&decimal_a, &decimal_b).ok()?
            } else {
                return_decimal_type
            };

            let mut scale_a = 0;
            let mut scale_b = 0;

            if is_multiply {
                scale_b = return_decimal_type.scale() as u32;
            } else if is_divide {
                scale_a = common_decimal_type.scale() as u32;
            }

            let function = Function {
                signature: FunctionSignature {
                    name: $name.to_string(),
                    args_type: args_type.clone(),
                    return_type: DataType::Decimal(return_decimal_type),
                },
                eval: FunctionEval::Scalar {
                    calc_domain: Box::new(move |_, d| {
                        let size = common_decimal_type.size();
                        let lhs = convert_to_decimal_domain(&d[0], common_decimal_type);
                        if lhs.is_none() {
                            return FunctionDomain::Full;
                        }
                        let rhs = convert_to_decimal_domain(&d[1], common_decimal_type);
                        if rhs.is_none() {
                            return FunctionDomain::Full;
                        }
                        let lhs = lhs.unwrap();
                        let rhs = rhs.unwrap();
                        {
                            match (lhs, rhs) {
                                (
                                    DecimalDomain::Decimal128(d1, s1),
                                    DecimalDomain::Decimal128(d2, s2),
                                ) if s1 == s2 => {
                                    $domain_op(&d1, &d2, scale_a, scale_b, size.precision)
                                        .map(|d| DecimalDomain::Decimal128(d, size))
                                }
                                (
                                    DecimalDomain::Decimal256(d1, s1),
                                    DecimalDomain::Decimal256(d2, s2),
                                ) if s1 == s2 => {
                                    $domain_op(&d1, &d2, scale_a, scale_b, size.precision)
                                        .map(|d| DecimalDomain::Decimal256(d, size))
                                }
                                _ => unreachable!(),
                            }
                        }
                        .and_then(|d| {
                            if common_decimal_type != return_decimal_type {
                                decimal_to_decimal_domain(&d, &return_decimal_type)
                            } else {
                                Some(d)
                            }
                        })
                        .map(|d| FunctionDomain::Domain(Domain::Decimal(d)))
                        .unwrap_or(FunctionDomain::Full)
                    }),
                    eval: Box::new(move |args, ctx| {
                        let lhs =
                            convert_to_decimal(&args[0], ctx, &args_type[0], common_decimal_type);

                        let rhs =
                            convert_to_decimal(&args[1], ctx, &args_type[1], common_decimal_type);

                        let res = op_decimal!(
                            &lhs.as_ref(),
                            &rhs.as_ref(),
                            ctx,
                            common_decimal_type,
                            $op,
                            scale_a,
                            scale_b,
                            is_divide
                        );

                        if common_decimal_type != return_decimal_type {
                            decimal_to_decimal(
                                &res.as_ref(),
                                ctx,
                                common_decimal_type,
                                return_decimal_type,
                            )
                        } else {
                            res
                        }
                    }),
                },
            };
            if has_nullable {
                Some(Arc::new(function.wrap_nullable()))
            } else {
                Some(Arc::new(function))
            }
        });
    };
}

pub(crate) fn register_decimal_compare_op(registry: &mut FunctionRegistry) {
    register_decimal_compare_op!(registry, "lt", is_lt, domain_lt);
    register_decimal_compare_op!(registry, "eq", is_eq, domain_eq);
    register_decimal_compare_op!(registry, "gt", is_gt, domain_gt);
    register_decimal_compare_op!(registry, "lte", is_le, domain_lte);
    register_decimal_compare_op!(registry, "gte", is_ge, domain_gte);
    register_decimal_compare_op!(registry, "ne", is_ne, domain_noteq);
}

pub(crate) fn register_decimal_arithmetic(registry: &mut FunctionRegistry) {
    // TODO checked overflow by default
    register_decimal_binary_op!(registry, "plus", add, domain_plus);
    register_decimal_binary_op!(registry, "minus", sub, domain_minus);
    register_decimal_binary_op!(registry, "divide", div, domain_div);
    register_decimal_binary_op!(registry, "multiply", mul, domain_mul);
}

// int float to decimal
pub fn register(registry: &mut FunctionRegistry) {
    let factory = |params: &[usize], args_type: &[DataType]| {
        if args_type.len() != 1 {
            return None;
        }
        if params.len() != 2 {
            return None;
        }

        let from_type = args_type[0].remove_nullable();

        if !matches!(
            from_type,
            DataType::Number(_) | DataType::Decimal(_) | DataType::String
        ) {
            return None;
        }

        let decimal_size = DecimalSize {
            precision: params[0] as u8,
            scale: params[1] as u8,
        };

        let decimal_type = DecimalDataType::from_size(decimal_size).ok()?;

        Some(Function {
            signature: FunctionSignature {
                name: "to_decimal".to_string(),
                args_type: vec![from_type.clone()],
                return_type: DataType::Decimal(decimal_type),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(move |_, d| {
                    convert_to_decimal_domain(&d[0], decimal_type)
                        .map(|d| FunctionDomain::Domain(Domain::Decimal(d)))
                        .unwrap_or(FunctionDomain::Full)
                }),
                eval: Box::new(move |args, ctx| {
                    convert_to_decimal(&args[0], ctx, &from_type, decimal_type)
                }),
            },
        })
    };

    registry.register_function_factory("to_decimal", move |params, args_type| {
        Some(Arc::new(factory(params, args_type)?))
    });
    registry.register_function_factory("to_decimal", move |params, args_type| {
        let f = factory(params, args_type)?;
        Some(Arc::new(f.wrap_nullable()))
    });
    registry.register_function_factory("try_to_decimal", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_decimal".to_string();
        Some(Arc::new(f.error_to_null()))
    });
    registry.register_function_factory("try_to_decimal", move |params, args_type| {
        let mut f = factory(params, args_type)?;
        f.signature.name = "try_to_decimal".to_string();
        Some(Arc::new(f.error_to_null().wrap_nullable()))
    });
}

macro_rules! single_decimal128_to_float {
    ($from: expr, $size: expr, $to_type: ty) => {{
        let base: $to_type = 10.0;
        let div: $to_type = base.powi($size.scale as i32);
        let v: OrderedFloat<$to_type> = ($from as $to_type / div).into();
        v
    }};
}

macro_rules! single_decimal256_to_float {
    ($from: expr, $size: expr, $to_type: ty) => {{
        let base: $to_type = 10.0;
        let div: $to_type = base.powi($size.scale as i32);
        let v: OrderedFloat<$to_type> = (<$to_type>::from($from) / div).into();
        v
    }};
}

pub(crate) fn register_decimal_to_float64(registry: &mut FunctionRegistry) {
    registry.register_function_factory("to_float64", |_params, args_type| {
        if args_type.len() != 1 {
            return None;
        }

        let arg_type = args_type[0].remove_nullable();

        if !arg_type.is_decimal() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "to_float64".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: Float64Type::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, d| match d[0].as_decimal().unwrap() {
                    DecimalDomain::Decimal128(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float64(SimpleDomain {
                            min: single_decimal128_to_float! {d.min, size, f64},
                            max: single_decimal128_to_float! {d.max, size, f64},
                        }),
                    )),
                    DecimalDomain::Decimal256(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float64(SimpleDomain {
                            min: single_decimal256_to_float! {d.min, size, f64},
                            max: single_decimal256_to_float! {d.max, size, f64},
                        }),
                    )),
                }),
                eval: Box::new(move |args, tx| decimal_to_float64(&args[0], arg_type.clone(), tx)),
            },
        }))
    });
}

pub(crate) fn register_decimal_to_float32(registry: &mut FunctionRegistry) {
    registry.register_function_factory("to_float32", |_params, args_type| {
        if args_type.len() != 1 {
            return None;
        }

        let arg_type = args_type[0].remove_nullable();
        if !arg_type.is_decimal() {
            return None;
        }

        Some(Arc::new(Function {
            signature: FunctionSignature {
                name: "to_float32".to_string(),
                args_type: vec![arg_type.clone()],
                return_type: Float32Type::data_type(),
            },
            eval: FunctionEval::Scalar {
                calc_domain: Box::new(|_, d| match d[0].as_decimal().unwrap() {
                    DecimalDomain::Decimal128(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float32(SimpleDomain {
                            min: single_decimal128_to_float! {d.min, size, f32},
                            max: single_decimal128_to_float! {d.max, size, f32},
                        }),
                    )),
                    DecimalDomain::Decimal256(d, size) => FunctionDomain::Domain(Domain::Number(
                        NumberDomain::Float32(SimpleDomain {
                            min: single_decimal256_to_float! {d.min, size, f32},
                            max: single_decimal256_to_float! {d.max, size, f32},
                        }),
                    )),
                }),
                eval: Box::new(move |args, tx| decimal_to_float32(&args[0], arg_type.clone(), tx)),
            },
        }))
    });
}

fn convert_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: &DataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    match from_type {
        DataType::Number(ty) => {
            if ty.is_float() {
                float_to_decimal(arg, ctx, *ty, dest_type)
            } else {
                integer_to_decimal(arg, ctx, *ty, dest_type)
            }
        }
        DataType::Decimal(from) => decimal_to_decimal(arg, ctx, *from, dest_type),
        DataType::String => string_to_decimal(arg, ctx, dest_type),
        _ => unreachable!("to_decimal not support this DataType"),
    }
}

fn convert_to_decimal_domain(domain: &Domain, ty: DecimalDataType) -> Option<DecimalDomain> {
    match domain {
        Domain::Number(number_domain) => {
            with_integer_mapped_type!(|NUM_TYPE| match number_domain {
                NumberDomain::NUM_TYPE(d) => integer_to_decimal_domain(d, &ty),
                NumberDomain::Float32(d) => float_to_decimal_domain(d, &ty),
                NumberDomain::Float64(d) => float_to_decimal_domain(d, &ty),
            })
        }
        Domain::Decimal(d) => decimal_to_decimal_domain(d, &ty),
        Domain::String(d) => string_to_decimal_domain(d, &ty),
        _ => None,
    }
}

fn string_to_decimal_column<T: Decimal>(
    ctx: &mut EvalContext,
    string_column: &StringColumn,
    size: DecimalSize,
) -> DecimalColumn {
    let mut values = Vec::<T>::with_capacity(string_column.len());
    for (row, buf) in string_column.iter().enumerate() {
        match read_decimal_with_size::<T>(buf, size, true) {
            Ok((d, _)) => values.push(d),
            Err(e) => {
                ctx.set_error(row, e.message());
                values.push(T::zero())
            }
        }
    }
    T::to_column(values, size)
}

fn string_to_decimal_scalar<T: Decimal>(
    ctx: &mut EvalContext,
    string_buf: &[u8],
    size: DecimalSize,
) -> DecimalScalar {
    let value = match read_decimal_with_size::<T>(string_buf, size, true) {
        Ok((d, _)) => d,
        Err(e) => {
            ctx.set_error(0, e.message());
            T::zero()
        }
    };
    T::to_scalar(value, size)
}

fn string_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    match arg {
        ValueRef::Column(column) => {
            let string_column = StringType::try_downcast_column(column).unwrap();
            let column = match dest_type {
                DecimalDataType::Decimal128(size) => {
                    string_to_decimal_column::<i128>(ctx, &string_column, size)
                }
                DecimalDataType::Decimal256(size) => {
                    string_to_decimal_column::<i256>(ctx, &string_column, size)
                }
            };
            Value::Column(Column::Decimal(column))
        }
        ValueRef::Scalar(scalar) => {
            let buf = StringType::try_downcast_scalar(scalar).unwrap();
            let scalar = match dest_type {
                DecimalDataType::Decimal128(size) => {
                    string_to_decimal_scalar::<i128>(ctx, buf, size)
                }
                DecimalDataType::Decimal256(size) => {
                    string_to_decimal_scalar::<i128>(ctx, buf, size)
                }
            };
            Value::Scalar(Scalar::Decimal(scalar))
        }
    }
}

fn string_to_decimal_domain(
    from: &StringDomain,
    dest_type: &DecimalDataType,
) -> Option<DecimalDomain> {
    let min = &from.min;
    let max = from.max.as_ref()?;
    Some(match dest_type {
        DecimalDataType::Decimal128(size) => DecimalDomain::Decimal128(
            SimpleDomain {
                min: read_decimal_with_size::<i128>(min, *size, true).ok()?.0,
                max: read_decimal_with_size::<i128>(max, *size, true).ok()?.0,
            },
            *size,
        ),
        DecimalDataType::Decimal256(size) => DecimalDomain::Decimal256(
            SimpleDomain {
                min: read_decimal_with_size::<i256>(min, *size, true).ok()?.0,
                max: read_decimal_with_size::<i256>(max, *size, true).ok()?.0,
            },
            *size,
        ),
    })
}

fn integer_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: NumberDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &DataType::Number(from_type));
            builder.build()
        }
    };

    let result = with_integer_mapped_type!(|NUM_TYPE| match from_type {
        NumberDataType::NUM_TYPE => {
            let column = NumberType::<NUM_TYPE>::try_downcast_column(&column).unwrap();
            integer_to_decimal_internal(column, ctx, &dest_type)
        }
        _ => unreachable!(),
    });

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::Decimal(scalar))
    } else {
        Value::Column(Column::Decimal(result))
    }
}

macro_rules! m_integer_to_decimal {
    ($from: expr, $size: expr, $type_name: ty, $ctx: expr) => {
        let multiplier = <$type_name>::e($size.scale as u32);
        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        let values = $from
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let x = x.as_() * <$type_name>::one();
                let x = x.checked_mul(multiplier).and_then(|v| {
                    if v > max_for_precision || v < min_for_precision {
                        None
                    } else {
                        Some(v)
                    }
                });

                match x {
                    Some(x) => x,
                    None => {
                        $ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                        <$type_name>::one()
                    }
                }
            })
            .collect();
        <$type_name>::to_column(values, $size)
    };
}

fn integer_to_decimal_internal<T: Number + AsPrimitive<i128>>(
    from: Buffer<T>,
    ctx: &mut EvalContext,
    dest_type: &DecimalDataType,
) -> DecimalColumn {
    match dest_type {
        DecimalDataType::Decimal128(size) => {
            m_integer_to_decimal! {from, *size, i128, ctx}
        }
        DecimalDataType::Decimal256(size) => {
            m_integer_to_decimal! {from, *size, i256, ctx}
        }
    }
}

macro_rules! single_integer_to_decimal {
    ($from: expr, $size: expr, $type_name: ty) => {{
        let multiplier = <$type_name>::e($size.scale as u32);
        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        let x = $from.as_() * <$type_name>::one();
        x.checked_mul(multiplier).and_then(|v| {
            if v > max_for_precision || v < min_for_precision {
                None
            } else {
                Some(v)
            }
        })
    }};
}

fn integer_to_decimal_domain<T: Number + AsPrimitive<i128>>(
    from: &SimpleDomain<T>,
    dest_type: &DecimalDataType,
) -> Option<DecimalDomain> {
    Some(match dest_type {
        DecimalDataType::Decimal128(size) => DecimalDomain::Decimal128(
            SimpleDomain {
                min: single_integer_to_decimal! {from.min, *size, i128}?,
                max: single_integer_to_decimal! {from.max, *size, i128}?,
            },
            *size,
        ),
        DecimalDataType::Decimal256(size) => DecimalDomain::Decimal256(
            SimpleDomain {
                min: single_integer_to_decimal! {from.min, *size, i256}?,
                max: single_integer_to_decimal! {from.max, *size, i256}?,
            },
            *size,
        ),
    })
}

macro_rules! m_float_to_decimal {
    ($from: expr, $size: expr, $type_name: ty, $ctx: expr) => {
        let multiplier: f64 = (10_f64).powi($size.scale as i32).as_();

        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        let values = $from
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let x = <$type_name>::from_float(x.as_() * multiplier);
                if x > max_for_precision || x < min_for_precision {
                    $ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                    <$type_name>::one()
                } else {
                    x
                }
            })
            .collect();
        <$type_name>::to_column(values, $size)
    };
}

fn float_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: NumberDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &DataType::Number(from_type));
            builder.build()
        }
    };

    let result = match from_type {
        NumberDataType::Float32 => {
            let column = NumberType::<F32>::try_downcast_column(&column).unwrap();
            float_to_decimal_internal(column, ctx, &dest_type)
        }
        NumberDataType::Float64 => {
            let column = NumberType::<F64>::try_downcast_column(&column).unwrap();
            float_to_decimal_internal(column, ctx, &dest_type)
        }
        _ => unreachable!(),
    };
    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::Decimal(scalar))
    } else {
        Value::Column(Column::Decimal(result))
    }
}

fn float_to_decimal_internal<T: Number + AsPrimitive<f64>>(
    from: Buffer<T>,
    ctx: &mut EvalContext,
    dest_type: &DecimalDataType,
) -> DecimalColumn {
    match dest_type {
        DecimalDataType::Decimal128(size) => {
            m_float_to_decimal! {from, *size, i128, ctx}
        }
        DecimalDataType::Decimal256(size) => {
            m_float_to_decimal! {from, *size, i256, ctx}
        }
    }
}

macro_rules! single_float_to_decimal {
    ($from: expr, $size: expr, $type_name: ty) => {{
        let multiplier: f64 = (10_f64).powi($size.scale as i32).as_();

        let min_for_precision = <$type_name>::min_for_precision($size.precision);
        let max_for_precision = <$type_name>::max_for_precision($size.precision);

        let x = <$type_name>::from_float($from.as_() * multiplier);
        if x > max_for_precision || x < min_for_precision {
            None
        } else {
            Some(x)
        }
    }};
}

fn float_to_decimal_domain<T: Number + AsPrimitive<f64>>(
    from: &SimpleDomain<T>,
    dest_type: &DecimalDataType,
) -> Option<DecimalDomain> {
    Some(match dest_type {
        DecimalDataType::Decimal128(size) => DecimalDomain::Decimal128(
            SimpleDomain {
                min: single_float_to_decimal! {from.min, *size, i128}?,
                max: single_float_to_decimal! {from.max, *size, i128}?,
            },
            *size,
        ),
        DecimalDataType::Decimal256(size) => DecimalDomain::Decimal256(
            SimpleDomain {
                min: single_float_to_decimal! {from.min, *size, i256}?,
                max: single_float_to_decimal! {from.max, *size, i256}?,
            },
            *size,
        ),
    })
}

fn decimal_256_to_128(
    buffer: Buffer<i256>,
    from_size: DecimalSize,
    dest_size: DecimalSize,
    ctx: &mut EvalContext,
) -> DecimalColumn {
    let max = i128::max_for_precision(dest_size.precision);
    let min = i128::min_for_precision(dest_size.precision);

    let values = if dest_size.scale >= from_size.scale {
        let factor = i256::e((dest_size.scale - from_size.scale) as u32);
        buffer
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let x = x * i128::one();
                match x.checked_mul(factor) {
                    Some(x) if x <= max && x >= min => *x.low(),
                    _ => {
                        ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                        i128::one()
                    }
                }
            })
            .collect()
    } else {
        let factor = i256::e((from_size.scale - dest_size.scale) as u32);
        buffer
            .iter()
            .enumerate()
            .map(|(row, x)| {
                let x = x * i128::one();
                match x.checked_div(factor) {
                    Some(x) if x <= max && x >= min => *x.low(),
                    _ => {
                        ctx.set_error(row, concat!("Decimal overflow at line : ", line!()));
                        i128::one()
                    }
                }
            })
            .collect()
    };
    i128::to_column(values, dest_size)
}

macro_rules! m_decimal_to_decimal {
    ($from_size: expr, $dest_size: expr, $buffer: expr, $from_type_name: ty, $dest_type_name: ty, $ctx: expr) => {
        // faster path
        if $from_size.scale == $dest_size.scale && $from_size.precision <= $dest_size.precision {
            if <$from_type_name>::MAX == <$dest_type_name>::MAX {
                // 128 -> 128 or 256 -> 256
                <$from_type_name>::to_column_from_buffer($buffer, $dest_size)
            } else {
                // 128 -> 256
                let buffer = $buffer
                    .into_iter()
                    .map(|x| x * <$dest_type_name>::one())
                    .collect();
                <$dest_type_name>::to_column(buffer, $dest_size)
            }
        } else {
            let values = if $from_size.scale > $dest_size.scale {
                let factor = <$dest_type_name>::e(($from_size.scale - $dest_size.scale) as u32);
                $buffer
                    .iter()
                    .enumerate()
                    .map(|(row, x)| {
                        let x = x * <$dest_type_name>::one();
                        match x.checked_div(factor) {
                            Some(x) => x,
                            None => {
                                $ctx.set_error(
                                    row,
                                    concat!("Decimal overflow at line : ", line!()),
                                );
                                <$dest_type_name>::one()
                            }
                        }
                    })
                    .collect()
            } else {
                let factor = <$dest_type_name>::e(($dest_size.scale - $from_size.scale) as u32);
                let max = <$dest_type_name>::max_for_precision($dest_size.precision);
                let min = <$dest_type_name>::min_for_precision($dest_size.precision);
                $buffer
                    .iter()
                    .enumerate()
                    .map(|(row, x)| {
                        let x = x * <$dest_type_name>::one();
                        match x.checked_mul(factor) {
                            Some(x) if x <= max && x >= min => x as $dest_type_name,
                            _ => {
                                $ctx.set_error(
                                    row,
                                    concat!("Decimal overflow at line : ", line!()),
                                );
                                <$dest_type_name>::one()
                            }
                        }
                    })
                    .collect()
            };

            <$dest_type_name>::to_column(values, $dest_size)
        }
    };
}

fn decimal_to_decimal(
    arg: &ValueRef<AnyType>,
    ctx: &mut EvalContext,
    from_type: DecimalDataType,
    dest_type: DecimalDataType,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &DataType::Decimal(from_type));
            builder.build()
        }
    };

    let result: DecimalColumn = match (from_type, dest_type) {
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal128(dest_size)) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, dest_size, buffer, i128, i128, ctx}
        }
        (DecimalDataType::Decimal128(_), DecimalDataType::Decimal256(dest_size)) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, dest_size, buffer, i128, i256, ctx}
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal256(dest_size)) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();
            m_decimal_to_decimal! {from_size, dest_size, buffer, i256, i256, ctx}
        }
        (DecimalDataType::Decimal256(_), DecimalDataType::Decimal128(dest_size)) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();
            decimal_256_to_128(buffer, from_size, dest_size, ctx)
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(Scalar::Decimal(scalar))
    } else {
        Value::Column(Column::Decimal(result))
    }
}

macro_rules! single_decimal_to_decimal {
    ($from: expr, $from_size: expr, $dest_size: expr, $from_type_name: ty, $dest_type_name: ty) => {{
        // faster path
        if $from_size.scale == $dest_size.scale && $from_size.precision <= $dest_size.precision {
            // 128 -> 128 or 128 -> 256 or 256 -> 256
            Some($from * <$dest_type_name>::one())
        } else if $from_size.scale > $dest_size.scale {
            let factor = <$dest_type_name>::e(($from_size.scale - $dest_size.scale) as u32);
            let x = $from * <$dest_type_name>::one();
            x.checked_div(factor)
        } else {
            let factor = <$dest_type_name>::e(($dest_size.scale - $from_size.scale) as u32);
            let max = <$dest_type_name>::max_for_precision($dest_size.precision);
            let min = <$dest_type_name>::min_for_precision($dest_size.precision);
            let x = $from * <$dest_type_name>::one();
            match x.checked_mul(factor) {
                Some(x) if x <= max && x >= min => Some(x),
                _ => None,
            }
        }
    }};
}

fn single_decimal_256_to_128(
    from: i256,
    from_size: DecimalSize,
    dest_size: DecimalSize,
) -> Option<i128> {
    let max = i128::max_for_precision(dest_size.precision);
    let min = i128::min_for_precision(dest_size.precision);
    if dest_size.scale >= from_size.scale {
        let factor = i256::e((dest_size.scale - from_size.scale) as u32);
        let x = from * i128::one();
        match x.checked_mul(factor) {
            Some(x) if x <= max && x >= min => Some(*x.low()),
            _ => None,
        }
    } else {
        let factor = i256::e((from_size.scale - dest_size.scale) as u32);
        let x = from * i128::one();
        match x.checked_div(factor) {
            Some(x) if x <= max && x >= min => Some(*x.low()),
            _ => None,
        }
    }
}

fn decimal_to_decimal_domain(
    from: &DecimalDomain,
    dest_type: &DecimalDataType,
) -> Option<DecimalDomain> {
    Some(match (from, dest_type) {
        (DecimalDomain::Decimal128(d, from_size), DecimalDataType::Decimal128(dest_size)) => {
            DecimalDomain::Decimal128(
                SimpleDomain {
                    min: single_decimal_to_decimal! {d.min, *from_size, *dest_size, i128, i128}?,
                    max: single_decimal_to_decimal! {d.max, *from_size, *dest_size, i128, i128}?,
                },
                *dest_size,
            )
        }
        (DecimalDomain::Decimal128(d, from_size), DecimalDataType::Decimal256(dest_size)) => {
            DecimalDomain::Decimal256(
                SimpleDomain {
                    min: single_decimal_to_decimal! {d.min, *from_size, *dest_size, i128, i256}?,
                    max: single_decimal_to_decimal! {d.max, *from_size, *dest_size, i128, i256}?,
                },
                *dest_size,
            )
        }
        (DecimalDomain::Decimal256(d, from_size), DecimalDataType::Decimal256(dest_size)) => {
            DecimalDomain::Decimal256(
                SimpleDomain {
                    min: single_decimal_to_decimal! {d.min, *from_size, *dest_size, i256, i256}?,
                    max: single_decimal_to_decimal! {d.max, *from_size, *dest_size, i256, i256}?,
                },
                *dest_size,
            )
        }
        (DecimalDomain::Decimal256(d, from_size), DecimalDataType::Decimal128(dest_size)) => {
            DecimalDomain::Decimal128(
                SimpleDomain {
                    min: single_decimal_256_to_128(d.min, *from_size, *dest_size)?,
                    max: single_decimal_256_to_128(d.max, *from_size, *dest_size)?,
                },
                *dest_size,
            )
        }
    })
}

fn decimal_to_float64(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    _ctx: &mut EvalContext,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_decimal().unwrap();

    let result = match from_type {
        DecimalDataType::Decimal128(_) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();

            let div = 10_f64.powi(from_size.scale as i32);

            let values: Buffer<F64> = buffer.iter().map(|x| (*x as f64 / div).into()).collect();
            Float64Type::upcast_column(values)
        }

        DecimalDataType::Decimal256(_) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();

            let div = 10_f64.powi(from_size.scale as i32);

            let values: Buffer<F64> = buffer
                .iter()
                .map(|x| (f64::from(*x) / div).into())
                .collect();
            Float64Type::upcast_column(values)
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(scalar.to_owned())
    } else {
        Value::Column(result)
    }
}

fn decimal_to_float32(
    arg: &ValueRef<AnyType>,
    from_type: DataType,
    _ctx: &mut EvalContext,
) -> Value<AnyType> {
    let mut is_scalar = false;
    let column = match arg {
        ValueRef::Column(column) => column.clone(),
        ValueRef::Scalar(s) => {
            is_scalar = true;
            let builder = ColumnBuilder::repeat(s, 1, &from_type);
            builder.build()
        }
    };

    let from_type = from_type.as_decimal().unwrap();

    let result = match from_type {
        DecimalDataType::Decimal128(_) => {
            let (buffer, from_size) = i128::try_downcast_column(&column).unwrap();

            let div = 10_f32.powi(from_size.scale as i32);

            let values: Buffer<F32> = buffer.iter().map(|x| (*x as f32 / div).into()).collect();
            Float32Type::upcast_column(values)
        }

        DecimalDataType::Decimal256(_) => {
            let (buffer, from_size) = i256::try_downcast_column(&column).unwrap();

            let div = 10_f32.powi(from_size.scale as i32);

            let values: Buffer<F32> = buffer
                .iter()
                .map(|x| (f32::from(*x) / div).into())
                .collect();
            Float32Type::upcast_column(values)
        }
    };

    if is_scalar {
        let scalar = result.index(0).unwrap();
        Value::Scalar(scalar.to_owned())
    } else {
        Value::Column(result)
    }
}
