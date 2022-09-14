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

use std::cmp::Ordering;
use std::f64::consts::E;
use std::f64::consts::PI;
use std::marker::PhantomData;

use common_expression::types::number::SimpleDomain;
use common_expression::types::number::F64;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::with_number_mapped_type;
use common_expression::FunctionProperty;
use common_expression::FunctionRegistry;
use common_expression::Value;
use num_traits::AsPrimitive;
use num_traits::Float;
use num_traits::Pow;
use ordered_float::OrderedFloat;
use rand::Rng;
use rand::SeedableRng;

use crate::scalars::ALL_FLOAT_TYPES;
use crate::scalars::ALL_INTEGER_TYPES;
use crate::scalars::ALL_NUMERICS_TYPES;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "sin",
        FunctionProperty::default(),
        |_| {
            Some(SimpleDomain {
                min: OrderedFloat(-1.0),
                max: OrderedFloat(1.0),
            })
        },
        |f: F64| f.sin(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "cos",
        FunctionProperty::default(),
        |_| {
            Some(SimpleDomain {
                min: OrderedFloat(-1.0),
                max: OrderedFloat(1.0),
            })
        },
        |f: F64| f.cos(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "tan",
        FunctionProperty::default(),
        |_| None,
        |f: F64| f.tan(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "cot",
        FunctionProperty::default(),
        |_| None,
        |f: F64| OrderedFloat(1.0f64) / f.tan(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "acos",
        FunctionProperty::default(),
        |_| {
            Some(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(PI),
            })
        },
        |f: F64| f.acos(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "asin",
        FunctionProperty::default(),
        |_| {
            Some(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(2.0 * PI),
            })
        },
        |f: F64| f.asin(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "atan",
        FunctionProperty::default(),
        |_| {
            Some(SimpleDomain {
                min: OrderedFloat(-PI / 2.0),
                max: OrderedFloat(PI / 2.0),
            })
        },
        |f: F64| f.atan(),
    );

    registry.register_2_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, _, _>(
        "atan2",
        FunctionProperty::default(),
        |_, _| {
            Some(SimpleDomain {
                min: OrderedFloat(-PI),
                max: OrderedFloat(PI),
            })
        },
        |f: F64, r: F64| f.atan2(r),
    );

    registry.register_0_arg_core::<NumberType<F64>, _, _>(
        "pi",
        FunctionProperty::default(),
        || {
            Some(SimpleDomain {
                min: OrderedFloat(PI),
                max: OrderedFloat(PI),
            })
        },
        |_| Ok(Value::Scalar(OrderedFloat(PI))),
    );

    let sign = |val: F64| match val.partial_cmp(&OrderedFloat(0.0f64)) {
        Some(Ordering::Greater) => 1,
        Some(Ordering::Less) => -1,
        _ => 0,
    };

    registry.register_1_arg::<NumberType<F64>, NumberType<i8>, _, _>(
        "sign",
        FunctionProperty::default(),
        move |domain| {
            Some(SimpleDomain {
                min: sign(domain.min),
                max: sign(domain.max),
            })
        },
        sign,
    );

    registry.register_1_arg::<NumberType<u64>, NumberType<u64>, _, _>(
        "abs",
        FunctionProperty::default(),
        |domain| Some(domain.clone()),
        |val| val,
    );

    registry.register_1_arg::<NumberType<i64>, NumberType<u64>, _, _>(
        "abs",
        FunctionProperty::default(),
        |domain| {
            let max = domain.min.unsigned_abs().max(domain.max.unsigned_abs());
            let mut min = domain.min.unsigned_abs().min(domain.max.unsigned_abs());

            if domain.max >= 0 && domain.min <= 0 {
                min = 0;
            }
            Some(SimpleDomain { min, max })
        },
        |val| val.unsigned_abs(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "abs",
        FunctionProperty::default(),
        |domain| {
            let max = Ord::max(domain.min.abs(), domain.max.abs());
            let mut min = Ord::min(domain.min.abs(), domain.max.abs());

            if domain.max > OrderedFloat(0.0) && domain.min <= OrderedFloat(0.0) {
                min = OrderedFloat(0.0);
            }
            Some(SimpleDomain { min, max })
        },
        |val| val.abs(),
    );

    for ty in ALL_INTEGER_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<NUM_TYPE>, _, _>(
                    "ceil",
                    FunctionProperty::default(),
                    |_| None,
                    |val| val,
                );
            }
        })
    }

    for ty in ALL_FLOAT_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "ceil",
                    FunctionProperty::default(),
                    |_| None,
                    |val| (val.as_(): F64).ceil(),
                );
            }
        })
    }

    registry.register_aliases("ceil", &["ceiling"]);

    registry.register_1_arg::<StringType, NumberType<u32>, _, _>(
        "crc32",
        FunctionProperty::default(),
        |_| None,
        crc32fast::hash,
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "degrees",
        FunctionProperty::default(),
        |_| None,
        |val| val.to_degrees(),
    );

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "radians",
        FunctionProperty::default(),
        |_| None,
        |val| val.to_radians(),
    );

    for ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "exp",
                    FunctionProperty::default(),
                    |_| None,
                    |val| (val.as_(): F64).exp(),
                );
            }
        })
    }

    registry.register_1_arg::<NumberType<F64>, NumberType<F64>, _, _>(
        "floor",
        FunctionProperty::default(),
        |_| None,
        |val| val.floor(),
    );

    registry.register_2_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, _, _>(
        "pow",
        FunctionProperty::default(),
        |_, _| None,
        |val, rhs| OrderedFloat(val.0.pow(rhs.0)),
    );

    registry.register_0_arg_core::<NumberType<F64>, _, _>(
        "rand",
        FunctionProperty::default(),
        || {
            Some(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(1.0),
            })
        },
        |_| {
            let mut rng = rand::rngs::SmallRng::from_entropy();
            Ok(Value::Scalar(rng.gen::<F64>()))
        },
    );

    registry.register_1_arg::<NumberType<u64>, NumberType<F64>, _, _>(
        "rand",
        FunctionProperty::default(),
        |_| {
            Some(SimpleDomain {
                min: OrderedFloat(0.0),
                max: OrderedFloat(1.0),
            })
        },
        |val| {
            let mut rng = rand::rngs::SmallRng::seed_from_u64(val);
            rng.gen::<F64>()
        },
    );

    for ty in ALL_NUMERICS_TYPES {
        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "round",
                    FunctionProperty::default(),
                    |_| None,
                    |val| (val.as_(): F64).round(),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_2_arg::<NumberType<NUM_TYPE>, NumberType<i64>, NumberType<F64>, _, _>(
                        "round",
                        FunctionProperty::default(),
                        |_, _| None,
                        |val, to| match to.cmp(&0) {
                            Ordering::Greater => {
                                let z = 10_f64.powi(if to > 30 { 30 } else { to as i32 });
                                (val.as_(): F64 * z).round() / z
                            }
                            Ordering::Less => {
                                let z = 10_f64.powi(if to < -30 { 30 } else { -to as i32 });
                                (val.as_(): F64 / z).round() * z
                            }
                            Ordering::Equal => (val.as_(): F64).round(),
                        },
                    );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "truncate",
                    FunctionProperty::default(),
                    |_| None,
                    |val| (val.as_(): F64).trunc(),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_2_arg::<NumberType<NUM_TYPE>, NumberType<i64>, NumberType<F64>, _, _>(
                        "truncate",
                        FunctionProperty::default(),
                        |_, _| None,
                        |val, to| match to.cmp(&0) {
                            Ordering::Greater => {
                                let z = 10_f64.powi(if to > 30 { 30 } else { to as i32 });
                                (val.as_(): F64 * z).trunc() / z
                            }
                            Ordering::Less => {
                                let z = 10_f64.powi(if to < -30 { 30 } else { -to as i32 });
                                (val.as_(): F64 / z).trunc() * z
                            }
                            Ordering::Equal => (val.as_(): F64).trunc(),
                        },
                    );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "sqrt",
                    FunctionProperty::default(),
                    |_| None,
                    |val| (val.as_(): F64).sqrt(),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "ln",
                    FunctionProperty::default(),
                    |_| None,
                    |val| LnFunction::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "log2",
                    FunctionProperty::default(),
                    |_| None,
                    |val| Log2Function::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "log10",
                    FunctionProperty::default(),
                    |_| None,
                    |val| Log10Function::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry.register_1_arg::<NumberType<NUM_TYPE>, NumberType<F64>, _, _>(
                    "log",
                    FunctionProperty::default(),
                    |_| None,
                    |val| LogFunction::log(val),
                );
            }
        });

        with_number_mapped_type!(|NUM_TYPE| match ty {
            NumberDataType::NUM_TYPE => {
                registry
                    .register_2_arg::<NumberType<NUM_TYPE>, NumberType<F64>, NumberType<F64>, _, _>(
                        "log",
                        FunctionProperty::default(),
                        |_, _| None,
                        |base, val| LogFunction::log_with_base(base, val),
                    );
            }
        });
    }
}

/// Const f64 is now allowed.
/// feature(adt_const_params) is not stable & complete
trait Base: Send + Sync + Clone + 'static {
    fn base() -> F64;
}

#[derive(Clone)]
struct EBase;

#[derive(Clone)]
struct TenBase;

#[derive(Clone)]
struct TwoBase;

impl Base for EBase {
    fn base() -> F64 {
        OrderedFloat(E)
    }
}

impl Base for TenBase {
    fn base() -> F64 {
        OrderedFloat(10f64)
    }
}

impl Base for TwoBase {
    fn base() -> F64 {
        OrderedFloat(2f64)
    }
}
#[derive(Clone)]
struct GenericLogFunction<T> {
    t: PhantomData<T>,
}

impl<T: Base> GenericLogFunction<T> {
    fn log<S>(val: S) -> F64
    where S: AsPrimitive<F64> {
        val.as_().log(T::base())
    }

    fn log_with_base<S, B>(base: S, val: B) -> F64
    where
        S: AsPrimitive<F64>,
        B: AsPrimitive<F64>,
    {
        val.as_().log(base.as_())
    }
}

type LnFunction = GenericLogFunction<EBase>;
type LogFunction = GenericLogFunction<EBase>;
type Log10Function = GenericLogFunction<TenBase>;
type Log2Function = GenericLogFunction<TwoBase>;
