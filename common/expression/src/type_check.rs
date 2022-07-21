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

use std::collections::HashMap;
use std::fmt::Write;

use itertools::Itertools;

use crate::error::Result;
use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::expression::Span;
use crate::function::FunctionRegistry;
use crate::function::FunctionSignature;
use crate::types::DataType;

impl DataType {
    fn number_type_info(&self) -> Option<(i8, bool, bool)> {
        match self {
            DataType::UInt8 => Some((1, false, false)),
            DataType::UInt16 => Some((2, false, false)),
            DataType::UInt32 => Some((4, false, false)),
            DataType::UInt64 => Some((8, false, false)),
            DataType::Int8 => Some((1, true, false)),
            DataType::Int16 => Some((2, true, false)),
            DataType::Int32 => Some((4, true, false)),
            DataType::Int64 => Some((8, true, false)),
            DataType::Float32 => Some((4, true, true)),
            DataType::Float64 => Some((8, true, true)),
            _ => None,
        }
    }
}

pub fn check(ast: &RawExpr, fn_registry: &FunctionRegistry) -> Result<(Expr, DataType)> {
    match ast {
        RawExpr::Literal { span, lit } => {
            let ty = check_literal(lit);
            Ok((
                Expr::Literal {
                    span: span.clone(),
                    lit: lit.clone(),
                },
                ty,
            ))
        }
        RawExpr::ColumnRef {
            span,
            id,
            data_type,
        } => Ok((
            Expr::ColumnRef {
                span: span.clone(),
                id: *id,
            },
            data_type.clone(),
        )),
        RawExpr::FunctionCall {
            span,
            name,
            args,
            params,
        } => {
            let (mut args_expr, mut args_type) = (
                Vec::with_capacity(args.len()),
                Vec::with_capacity(args.len()),
            );

            for arg in args {
                let (arg, ty) = check(arg, fn_registry)?;
                args_expr.push(arg);
                args_type.push(ty);
            }

            check_function(
                span.clone(),
                name,
                params,
                &args_expr,
                &args_type,
                fn_registry,
            )
        }
    }
}

pub fn check_literal(literal: &Literal) -> DataType {
    match literal {
        Literal::Null => DataType::Null,
        Literal::Int8(_) => DataType::Int8,
        Literal::Int16(_) => DataType::Int16,
        Literal::Int32(_) => DataType::Int32,
        Literal::Int64(_) => DataType::Int64,
        Literal::UInt8(_) => DataType::UInt8,
        Literal::UInt16(_) => DataType::UInt16,
        Literal::UInt32(_) => DataType::UInt32,
        Literal::UInt64(_) => DataType::UInt64,
        Literal::Float32(_) => DataType::Float32,
        Literal::Float64(_) => DataType::Float64,
        Literal::Boolean(_) => DataType::Boolean,
        Literal::String(_) => DataType::String,
    }
}

pub fn check_function(
    span: Span,
    name: &str,
    params: &[usize],
    args: &[Expr],
    args_type: &[DataType],
    fn_registry: &FunctionRegistry,
) -> Result<(Expr, DataType)> {
    let candidates = fn_registry.search_candidates(name, params, args_type);

    let mut fail_resaons = Vec::with_capacity(candidates.len());
    for (id, func) in &candidates {
        match try_check_function(span.clone(), args, args_type, &func.signature) {
            Ok((checked_args, return_ty, generics)) => {
                return Ok((
                    Expr::FunctionCall {
                        span,
                        id: id.clone(),
                        function: func.clone(),
                        generics,
                        args: checked_args,
                    },
                    return_ty,
                ));
            }
            Err(err) => fail_resaons.push(err),
        }
    }

    let mut msg = if params.is_empty() {
        format!(
            "no overload satisfies `{name}({})`",
            args_type.iter().map(ToString::to_string).join(", ")
        )
    } else {
        format!(
            "no overload satisfies `{name}({})({})`",
            params.iter().join(", "),
            args_type.iter().map(ToString::to_string).join(", ")
        )
    };
    if !candidates.is_empty() {
        let candidates_sig: Vec<_> = candidates
            .iter()
            .map(|(_, func)| func.signature.to_string())
            .collect();

        let max_len = candidates_sig.iter().map(|s| s.len()).max().unwrap_or(0);

        let candidates_fail_reason = candidates_sig
            .into_iter()
            .zip(fail_resaons)
            .map(|(sig, (_, reason))| format!("  {sig:<max_len$}  : {reason}"))
            .join("\n");

        write!(
            &mut msg,
            "\n\nhas tried possible overloads:\n{}",
            candidates_fail_reason
        )
        .unwrap();
    };

    Err((span, msg))
}

#[derive(Debug)]
pub struct Subsitution(pub HashMap<usize, DataType>);

impl Subsitution {
    pub fn empty() -> Self {
        Subsitution(HashMap::new())
    }

    pub fn equation(idx: usize, ty: DataType) -> Self {
        let mut subst = Self::empty();
        subst.0.insert(idx, ty);
        subst
    }

    pub fn merge(mut self, other: Self) -> Result<Self> {
        for (idx, ty2) in other.0 {
            if let Some(ty1) = self.0.remove(&idx) {
                let common_ty = common_super_type(ty2.clone(), ty1.clone()).ok_or_else(|| {
                    (
                        None,
                        (format!("unable to find a common super type for `{ty1}` and `{ty2}`")),
                    )
                })?;
                self.0.insert(idx, common_ty);
            } else {
                self.0.insert(idx, ty2);
            }
        }

        Ok(self)
    }

    pub fn apply(&self, ty: DataType) -> Result<DataType> {
        match ty {
            DataType::Generic(idx) => self
                .0
                .get(&idx)
                .cloned()
                .ok_or_else(|| (None, (format!("unbound generic type `T{idx}`")))),
            DataType::Nullable(box ty) => Ok(DataType::Nullable(Box::new(self.apply(ty)?))),
            DataType::Array(box ty) => Ok(DataType::Array(Box::new(self.apply(ty)?))),
            ty => Ok(ty),
        }
    }
}

#[allow(clippy::type_complexity)]
pub fn try_check_function(
    span: Span,
    args: &[Expr],
    args_type: &[DataType],
    sig: &FunctionSignature,
) -> Result<(Vec<Expr>, DataType, Vec<DataType>)> {
    assert_eq!(args.len(), sig.args_type.len());

    let substs = args_type
        .iter()
        .zip(&sig.args_type)
        .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty).map_err(|(_, err)| (span.clone(), err)))
        .collect::<Result<Vec<_>>>()?;
    let subst = substs
        .into_iter()
        .try_reduce(|subst1, subst2| subst1.merge(subst2).map_err(|(_, err)| (span.clone(), err)))?
        .unwrap_or_else(Subsitution::empty);

    let checked_args = args
        .iter()
        .zip(args_type)
        .zip(&sig.args_type)
        .map(|((arg, arg_type), sig_type)| {
            let sig_type = subst.apply(sig_type.clone())?;
            Ok(if *arg_type == sig_type {
                arg.clone()
            } else {
                Expr::Cast {
                    span: span.clone(),
                    expr: Box::new(arg.clone()),
                    dest_type: sig_type,
                }
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let return_type = subst.apply(sig.return_type.clone())?;

    let generics = subst
        .0
        .keys()
        .cloned()
        .max()
        .map(|max_generic_idx| {
            (0..max_generic_idx + 1)
                .map(|idx| match subst.0.get(&idx) {
                    Some(ty) => ty.clone(),
                    None => DataType::Generic(idx),
                })
                .collect()
        })
        .unwrap_or_default();

    Ok((checked_args, return_type, generics))
}

pub fn unify(src_ty: &DataType, dest_ty: &DataType) -> Result<Subsitution> {
    match (src_ty, dest_ty) {
        (DataType::Generic(_), _) => unreachable!("source type must not contain generic type"),
        (ty, DataType::Generic(idx)) => Ok(Subsitution::equation(*idx, ty.clone())),
        (DataType::Null, DataType::Nullable(_)) => Ok(Subsitution::empty()),
        (DataType::EmptyArray, DataType::Array(_)) => Ok(Subsitution::empty()),
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty),
        (src_ty, DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => unify(src_ty, dest_ty),
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys))
            if src_tys.len() == dest_tys.len() =>
        {
            let substs = src_tys
                .iter()
                .zip(dest_tys)
                .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty))
                .collect::<Result<Vec<_>>>()?;
            let subst = substs
                .into_iter()
                .try_reduce(|subst1, subst2| subst1.merge(subst2))?
                .unwrap_or_else(Subsitution::empty);
            Ok(subst)
        }
        (src_ty, dest_ty) if can_cast_to(src_ty, dest_ty) => Ok(Subsitution::empty()),
        _ => Err((
            None,
            (format!("unable to unify `{}` with `{}`", src_ty, dest_ty)),
        )),
    }
}

// TODO: should support fallable casts
pub fn can_cast_to(src_ty: &DataType, dest_ty: &DataType) -> bool {
    match (src_ty, dest_ty) {
        (src_ty, dest_ty) if src_ty == dest_ty => true,
        (DataType::Null, DataType::Nullable(_)) => true,
        (DataType::EmptyArray, DataType::Array(_)) => true,
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => can_cast_to(src_ty, dest_ty),
        (src_ty, DataType::Nullable(dest_ty)) => can_cast_to(src_ty, dest_ty),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => can_cast_to(src_ty, dest_ty),
        (src_ty, dest_ty) => match (src_ty.number_type_info(), dest_ty.number_type_info()) {
            (Some((size1, b1, false)), Some((size2, b2, false))) if b1 == b2 => size1 <= size2,
            (Some((size1, false, false)), Some((size2, true, false))) if size2 > size1 => true,
            (Some((size1, _, true)), Some((size2, _, true))) => size1 <= size2,
            (Some((size1, _, false)), Some((size2, _, true))) if size2 > size1 => true,
            _ => false,
        },
    }
}

pub fn common_super_type(ty1: DataType, ty2: DataType) -> Option<DataType> {
    match (ty1, ty2) {
        (ty1, ty2) if ty1 == ty2 => Some(ty1),
        (DataType::Null, ty @ DataType::Nullable(_))
        | (ty @ DataType::Nullable(_), DataType::Null) => Some(ty),
        (DataType::Null, ty) | (ty, DataType::Null) => Some(DataType::Nullable(Box::new(ty))),
        (DataType::Nullable(box ty1), DataType::Nullable(box ty2))
        | (DataType::Nullable(box ty1), ty2)
        | (ty1, DataType::Nullable(box ty2)) => {
            Some(DataType::Nullable(Box::new(common_super_type(ty1, ty2)?)))
        }
        (DataType::EmptyArray, ty @ DataType::Array(_))
        | (ty @ DataType::Array(_), DataType::EmptyArray) => Some(ty),
        (DataType::Array(box ty1), DataType::Array(box ty2)) => {
            Some(DataType::Array(Box::new(common_super_type(ty1, ty2)?)))
        }
        (ty1, ty2) => match (ty1.number_type_info(), ty2.number_type_info()) {
            (Some((size1, b1, false)), Some((size2, b2, false))) if b1 == b2 => {
                (size1 >= size2).then(|| Some(ty1)).unwrap_or(Some(ty2))
            }
            (Some((size1, false, false)), Some((size2, true, false))) if size2 > size1 => Some(ty2),
            (Some((size1, true, false)), Some((size2, false, false))) if size1 > size2 => Some(ty1),
            (Some((1, _, false)), Some((1, _, false))) => Some(DataType::Int16),
            (Some((2, _, false)), Some((2, _, false))) => Some(DataType::Int32),
            (Some((4, _, false)), Some((4, _, false))) => Some(DataType::Int64),
            (Some((size1, _, true)), Some((size2, _, true))) => {
                (size1 >= size2).then(|| Some(ty1)).unwrap_or(Some(ty2))
            }
            (Some((size1, _, false)), Some((size2, _, true))) if size2 > size1 => Some(ty2),
            (Some((size1, _, true)), Some((size2, _, false))) if size1 > size2 => Some(ty1),
            (Some((4, _, _)), Some((4, _, _))) => Some(DataType::Float64),
            _ => None,
        },
    }
}
