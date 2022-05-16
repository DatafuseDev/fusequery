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

use common_datavalues::IntervalKind;
use itertools::Itertools;
use nom::branch::alt;
use nom::combinator::consumed;
use nom::combinator::map;
use nom::combinator::value;
use nom::error::context;
use nom::Slice as _;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattError;
use pratt::PrattParser;
use pratt::Precedence;

use crate::ast::*;
use crate::parser::error::Error;
use crate::parser::error::ErrorKind;
use crate::parser::query::*;
use crate::parser::token::*;
use crate::parser::util::*;
use crate::rule;

pub const BETWEEN_PREC: u32 = 20;
pub const NOT_PREC: u32 = 15;

pub fn expr(i: Input) -> IResult<Expr> {
    context("expression", subexpr(0))(i)
}

pub fn subexpr(min_precedence: u32) -> impl FnMut(Input) -> IResult<Expr> {
    move |i| {
        let higher_prec_expr_element =
            |i| {
                expr_element(i).and_then(|(rest, elem)| {
                    match PrattParser::<std::iter::Once<_>>::query(&mut ExprParser, &elem).unwrap()
                    {
                        Affix::Infix(prec, _) | Affix::Prefix(prec) | Affix::Postfix(prec)
                            if prec <= Precedence(min_precedence) =>
                        {
                            Err(nom::Err::Error(Error::from_error_kind(
                                i,
                                ErrorKind::Other("expected more tokens for expression"),
                            )))
                        }
                        _ => Ok((rest, elem)),
                    }
                })
            };

        let (rest, mut expr_elements) = rule! { #higher_prec_expr_element+ }(i)?;

        // Replace binary Plus and Minus to the unary one, if it's following another op or it's the first element.
        for (prev, curr) in (-1..(expr_elements.len() as isize)).tuple_windows() {
            if prev == -1
                || matches!(
                    expr_elements[prev as usize].elem,
                    ExprElement::UnaryOp { .. } | ExprElement::BinaryOp { .. }
                )
            {
                match &mut expr_elements[curr as usize].elem {
                    elem @ ExprElement::BinaryOp {
                        op: BinaryOperator::Plus,
                    } => {
                        *elem = ExprElement::UnaryOp {
                            op: UnaryOperator::Plus,
                        };
                    }
                    elem @ ExprElement::BinaryOp {
                        op: BinaryOperator::Minus,
                    } => {
                        *elem = ExprElement::UnaryOp {
                            op: UnaryOperator::Minus,
                        };
                    }
                    _ => {}
                }
            }
        }

        let mut iter = expr_elements.into_iter();
        let expr = ExprParser
            .parse(&mut iter)
            .map_err(|err| {
                map_pratt_error(
                    iter.next()
                        .map(|elem| elem.span)
                        // It's safe to slice one more token because EOI is always added.
                        .unwrap_or_else(|| rest.slice(..1)),
                    err,
                )
            })
            .map_err(nom::Err::Error)?;

        if let Some(elem) = iter.next() {
            return Err(nom::Err::Error(Error::from_error_kind(
                elem.span,
                ErrorKind::Other("unable to parse rest of the expression"),
            )));
        }

        Ok((rest, expr))
    }
}

fn map_pratt_error<'a>(
    next_token: Input<'a>,
    err: PrattError<WithSpan<'a>, pratt::NoError>,
) -> Error<'a> {
    match err {
        PrattError::EmptyInput => Error::from_error_kind(
            next_token,
            ErrorKind::Other("expected more tokens for expression"),
        ),
        PrattError::UnexpectedNilfix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the expression value"),
        ),
        PrattError::UnexpectedPrefix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the prefix operator"),
        ),
        PrattError::UnexpectedInfix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the binary operator"),
        ),
        PrattError::UnexpectedPostfix(elem) => Error::from_error_kind(
            elem.span,
            ErrorKind::Other("unable to parse the postfix operator"),
        ),
        PrattError::UserError(_) => unreachable!(),
    }
}

#[derive(Debug, Clone)]
pub struct WithSpan<'a> {
    span: Input<'a>,
    elem: ExprElement<'a>,
}

/// A 'flattened' AST of expressions.
///
/// This is used to parse expressions in Pratt parser.
/// The Pratt parser is not able to parse expressions by grammar. So we need to extract
/// the expression operands and operators to be the input of Pratt parser, by running a
/// nom parser in advance.
///
/// For example, `a + b AND c is null` is parsed as `[col(a), PLUS, col(b), AND, col(c), ISNULL]` by nom parsers.
/// Then the Pratt parser is able to parse the expression into `AND(PLUS(col(a), col(b)), ISNULL(col(c)))`.
#[derive(Debug, Clone, PartialEq)]
pub enum ExprElement<'a> {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        database: Option<Identifier<'a>>,
        table: Option<Identifier<'a>>,
        column: Identifier<'a>,
    },
    /// `IS NULL` expression
    IsNull {
        not: bool,
    },
    /// `IS NOT NULL` expression
    /// `[ NOT ] IN (list, ...)`
    InList {
        list: Vec<Expr<'a>>,
        not: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        subquery: Box<Query<'a>>,
        not: bool,
    },
    /// `BETWEEN ... AND ...`
    Between {
        low: Box<Expr<'a>>,
        high: Box<Expr<'a>>,
        not: bool,
    },
    /// Binary operation
    BinaryOp {
        op: BinaryOperator,
    },
    /// Unary operation
    UnaryOp {
        op: UnaryOperator,
    },
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast {
        expr: Box<Expr<'a>>,
        target_type: TypeName,
    },
    /// `TRY_CAST` expression`
    TryCast {
        expr: Box<Expr<'a>>,
        target_type: TypeName,
    },
    /// `::<type_name>` expression
    PgCast {
        target_type: TypeName,
    },
    /// EXTRACT(IntervalKind FROM <expr>)
    Extract {
        field: IntervalKind,
        expr: Box<Expr<'a>>,
    },
    /// POSITION(<expr> IN <expr>)
    Position {
        substr_expr: Box<Expr<'a>>,
        str_expr: Box<Expr<'a>>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    SubString {
        expr: Box<Expr<'a>>,
        substring_from: Option<Box<Expr<'a>>>,
        substring_for: Option<Box<Expr<'a>>>,
    },
    /// TRIM([[BOTH | LEADING | TRAILING] <expr> FROM] <expr>)
    /// Or
    /// TRIM(<expr>)
    Trim {
        expr: Box<Expr<'a>>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhere, Box<Expr<'a>>)>,
    },
    /// A literal value, such as string, number, date or NULL
    Literal {
        lit: Literal,
    },
    /// `Count(*)` expression
    CountAll,
    /// `(foo, bar)`
    Tuple {
        exprs: Vec<Expr<'a>>,
    },
    /// Scalar function call
    FunctionCall {
        /// Set to true if the function is aggregate function with `DISTINCT`, like `COUNT(DISTINCT a)`
        distinct: bool,
        name: Identifier<'a>,
        args: Vec<Expr<'a>>,
        params: Vec<Literal>,
    },
    /// `CASE ... WHEN ... ELSE ...` expression
    Case {
        operand: Option<Box<Expr<'a>>>,
        conditions: Vec<Expr<'a>>,
        results: Vec<Expr<'a>>,
        else_result: Option<Box<Expr<'a>>>,
    },
    /// `EXISTS` expression
    Exists {
        subquery: Query<'a>,
    },
    /// Scalar subquery, which will only return a single row with a single column.
    Subquery {
        subquery: Query<'a>,
    },
    /// Access elements of `Array`, `Object` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess {
        accessor: MapAccessor<'a>,
    },
    /// An expression between parentheses
    Group(Expr<'a>),
    DateTimeUnit {
        unit: IntervalKind,
    },
    /// `[1, 2, 3]`
    Array { exprs: Vec<Expr<'a>> },
}

struct ExprParser;

impl<'a, I: Iterator<Item = WithSpan<'a>>> PrattParser<I> for ExprParser {
    type Error = pratt::NoError;
    type Input = WithSpan<'a>;
    type Output = Expr<'a>;

    fn query(&mut self, elem: &WithSpan) -> pratt::Result<Affix> {
        let affix = match &elem.elem {
            ExprElement::MapAccess { .. } => Affix::Postfix(Precedence(10)),
            ExprElement::IsNull { .. } => Affix::Postfix(Precedence(17)),
            ExprElement::Between { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::InList { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::InSubquery { .. } => Affix::Postfix(Precedence(BETWEEN_PREC)),
            ExprElement::UnaryOp { op } => match op {
                UnaryOperator::Not => Affix::Prefix(Precedence(NOT_PREC)),

                UnaryOperator::Plus => Affix::Prefix(Precedence(30)),
                UnaryOperator::Minus => Affix::Prefix(Precedence(30)),
            },
            ExprElement::BinaryOp { op } => match op {
                BinaryOperator::Or => Affix::Infix(Precedence(5), Associativity::Left),

                BinaryOperator::And => Affix::Infix(Precedence(10), Associativity::Left),

                BinaryOperator::Eq => Affix::Infix(Precedence(20), Associativity::Right),
                BinaryOperator::NotEq => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::Gt => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::Lt => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::Gte => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::Lte => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::Like => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::NotLike => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::Regexp => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::NotRegexp => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::RLike => Affix::Infix(Precedence(20), Associativity::Left),
                BinaryOperator::NotRLike => Affix::Infix(Precedence(20), Associativity::Left),

                BinaryOperator::BitwiseOr => Affix::Infix(Precedence(22), Associativity::Left),
                BinaryOperator::BitwiseAnd => Affix::Infix(Precedence(22), Associativity::Left),
                BinaryOperator::BitwiseXor => Affix::Infix(Precedence(22), Associativity::Left),

                BinaryOperator::Xor => Affix::Infix(Precedence(24), Associativity::Left),

                BinaryOperator::Plus => Affix::Infix(Precedence(30), Associativity::Left),
                BinaryOperator::Minus => Affix::Infix(Precedence(30), Associativity::Left),

                BinaryOperator::Multiply => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Div => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Divide => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::Modulo => Affix::Infix(Precedence(40), Associativity::Left),
                BinaryOperator::StringConcat => Affix::Infix(Precedence(40), Associativity::Left),
            },
            ExprElement::PgCast { .. } => Affix::Postfix(Precedence(50)),
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, elem: WithSpan<'a>) -> pratt::Result<Expr<'a>> {
        let expr = match elem.elem {
            ExprElement::ColumnRef {
                database,
                table,
                column,
            } => Expr::ColumnRef {
                span: elem.span.0,
                database,
                table,
                column,
            },
            ExprElement::Cast { expr, target_type } => Expr::Cast {
                span: elem.span.0,
                expr,
                target_type,
                pg_style: false,
            },
            ExprElement::TryCast { expr, target_type } => Expr::TryCast {
                span: elem.span.0,
                expr,
                target_type,
            },
            ExprElement::Extract { field, expr } => Expr::Extract {
                span: elem.span.0,
                kind: field,
                expr,
            },
            ExprElement::Position {
                substr_expr,
                str_expr,
            } => Expr::Position {
                span: elem.span.0,
                substr_expr,
                str_expr,
            },
            ExprElement::SubString {
                expr,
                substring_from,
                substring_for,
            } => Expr::Substring {
                span: elem.span.0,
                expr,
                substring_from,
                substring_for,
            },
            ExprElement::Trim { expr, trim_where } => Expr::Trim {
                span: elem.span.0,
                expr,
                trim_where,
            },
            ExprElement::Literal { lit } => Expr::Literal {
                span: elem.span.0,
                lit,
            },
            ExprElement::CountAll => Expr::CountAll { span: elem.span.0 },
            ExprElement::Tuple { exprs } => Expr::Tuple {
                span: elem.span.0,
                exprs,
            },
            ExprElement::FunctionCall {
                distinct,
                name,
                args,
                params,
            } => Expr::FunctionCall {
                span: elem.span.0,
                distinct,
                name,
                args,
                params,
            },
            ExprElement::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Expr::Case {
                span: elem.span.0,
                operand,
                conditions,
                results,
                else_result,
            },
            ExprElement::Exists { subquery } => Expr::Exists {
                span: elem.span.0,
                subquery: Box::new(subquery),
            },
            ExprElement::Subquery { subquery } => Expr::Subquery {
                span: elem.span.0,
                subquery: Box::new(subquery),
            },
            ExprElement::Group(expr) => expr,
            ExprElement::DateTimeUnit { unit } => Expr::DateTimeUnit {
                span: elem.span.0,
                unit,
            },
            ExprElement::Array { exprs } => Expr::Array {
                span: elem.span.0,
                exprs,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn infix(
        &mut self,
        lhs: Expr<'a>,
        elem: WithSpan<'a>,
        rhs: Expr<'a>,
    ) -> pratt::Result<Expr<'a>> {
        let expr = match elem.elem {
            ExprElement::BinaryOp { op } => Expr::BinaryOp {
                span: elem.span.0,
                left: Box::new(lhs),
                right: Box::new(rhs),
                op,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn prefix(&mut self, elem: WithSpan<'a>, rhs: Expr<'a>) -> pratt::Result<Expr<'a>> {
        let expr = match elem.elem {
            ExprElement::UnaryOp { op } => Expr::UnaryOp {
                span: elem.span.0,
                op,
                expr: Box::new(rhs),
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }

    fn postfix(&mut self, lhs: Expr<'a>, elem: WithSpan<'a>) -> pratt::Result<Expr<'a>> {
        let expr = match elem.elem {
            ExprElement::MapAccess { accessor } => Expr::MapAccess {
                span: elem.span.0,
                expr: Box::new(lhs),
                accessor,
            },
            ExprElement::IsNull { not } => Expr::IsNull {
                span: elem.span.0,
                expr: Box::new(lhs),
                not,
            },
            ExprElement::InList { list, not } => Expr::InList {
                span: elem.span.0,
                expr: Box::new(lhs),
                list,
                not,
            },
            ExprElement::InSubquery { subquery, not } => Expr::InSubquery {
                span: elem.span.0,
                expr: Box::new(lhs),
                subquery,
                not,
            },
            ExprElement::Between { low, high, not } => Expr::Between {
                span: elem.span.0,
                expr: Box::new(lhs),
                low,
                high,
                not,
            },
            ExprElement::PgCast { target_type } => Expr::Cast {
                span: elem.span.0,
                expr: Box::new(lhs),
                target_type,
                pg_style: true,
            },
            _ => unreachable!(),
        };
        Ok(expr)
    }
}

pub fn expr_element(i: Input) -> IResult<WithSpan> {
    let column_ref = map(
        rule! {
            #ident ~ ("." ~ #ident ~ ("." ~ #ident)?)?
        },
        |res| match res {
            (column, None) => ExprElement::ColumnRef {
                database: None,
                table: None,
                column,
            },
            (table, Some((_, column, None))) => ExprElement::ColumnRef {
                database: None,
                table: Some(table),
                column,
            },
            (database, Some((_, table, Some((_, column))))) => ExprElement::ColumnRef {
                database: Some(database),
                table: Some(table),
                column,
            },
        },
    );
    let datetime_unit = map(
        rule! {YEAR|MONTH|DAY|HOUR|MINUTE|SECOND|DOY|DOW},
        |token| match token.kind {
            YEAR => ExprElement::DateTimeUnit {
                unit: IntervalKind::Year,
            },
            MONTH => ExprElement::DateTimeUnit {
                unit: IntervalKind::Month,
            },
            DAY => ExprElement::DateTimeUnit {
                unit: IntervalKind::Day,
            },
            HOUR => ExprElement::DateTimeUnit {
                unit: IntervalKind::Month,
            },
            MINUTE => ExprElement::DateTimeUnit {
                unit: IntervalKind::Minute,
            },
            SECOND => ExprElement::DateTimeUnit {
                unit: IntervalKind::Second,
            },
            _ => unimplemented!(),
        },
    );
    let is_null = map(
        rule! {
            IS ~ NOT? ~ NULL
        },
        |(_, opt_not, _)| ExprElement::IsNull {
            not: opt_not.is_some(),
        },
    );
    let in_list = map(
        rule! {
            NOT? ~ IN ~ "(" ~ #comma_separated_list1(subexpr(0)) ~ ^")"
        },
        |(opt_not, _, _, list, _)| ExprElement::InList {
            list,
            not: opt_not.is_some(),
        },
    );
    let in_subquery = map(
        rule! {
            NOT? ~ IN ~ "(" ~ #query  ~ ^")"
        },
        |(opt_not, _, _, subquery, _)| ExprElement::InSubquery {
            subquery: Box::new(subquery),
            not: opt_not.is_some(),
        },
    );
    let between = map(
        rule! {
            NOT? ~ BETWEEN ~ ^#subexpr(BETWEEN_PREC) ~ ^AND ~ ^#subexpr(BETWEEN_PREC)
        },
        |(opt_not, _, low, _, high)| ExprElement::Between {
            low: Box::new(low),
            high: Box::new(high),
            not: opt_not.is_some(),
        },
    );
    let cast = map(
        rule! {
            ( CAST | TRY_CAST )
            ~ "("
            ~ ^#subexpr(0)
            ~ ^( AS | "," )
            ~ ^#type_name
            ~ ^")"
        },
        |(cast, _, expr, _, target_type, _)| {
            if cast.kind == CAST {
                ExprElement::Cast {
                    expr: Box::new(expr),
                    target_type,
                }
            } else {
                ExprElement::TryCast {
                    expr: Box::new(expr),
                    target_type,
                }
            }
        },
    );
    let pg_cast = map(
        rule! {
            "::" ~ ^#type_name
        },
        |(_, target_type)| ExprElement::PgCast { target_type },
    );
    let extract = map(
        rule! {
            EXTRACT ~ ^"(" ~ ^#interval_kind ~ ^FROM ~ ^#subexpr(0) ~ ^")"
        },
        |(_, _, field, _, expr, _)| ExprElement::Extract {
            field,
            expr: Box::new(expr),
        },
    );
    let position = map(
        rule! {
            POSITION
            ~ ^"("
            ~ ^#subexpr(BETWEEN_PREC)
            ~ ^IN
            ~ ^#subexpr(0)
            ~ ^")"
        },
        |(_, _, substr_expr, _, str_expr, _)| ExprElement::Position {
            substr_expr: Box::new(substr_expr),
            str_expr: Box::new(str_expr),
        },
    );
    let substring = map(
        rule! {
            SUBSTRING
            ~ ^"("
            ~ ^#subexpr(0)
            ~ ( ( FROM | "," ) ~ ^#subexpr(0) )?
            ~ ( ( FOR | "," ) ~ ^#subexpr(0) )?
            ~ ^")"
        },
        |(_, _, expr, opt_substring_from, opt_substring_for, _)| ExprElement::SubString {
            expr: Box::new(expr),
            substring_from: opt_substring_from.map(|(_, expr)| Box::new(expr)),
            substring_for: opt_substring_for.map(|(_, expr)| Box::new(expr)),
        },
    );
    let trim_where = alt((
        value(TrimWhere::Both, rule! { BOTH }),
        value(TrimWhere::Leading, rule! { LEADING }),
        value(TrimWhere::Trailing, rule! { TRAILING }),
    ));
    let trim = map(
        rule! {
            TRIM
            ~ ^"("
            ~ #subexpr(0)
            ~ ^")"
        },
        |(_, _, expr, _)| ExprElement::Trim {
            expr: Box::new(expr),
            trim_where: None,
        },
    );
    let trim_from = map(
        rule! {
            TRIM
            ~ ^"("
            ~ #trim_where
            ~ ^#subexpr(0)
            ~ ^FROM
            ~ ^#subexpr(0)
            ~ ^")"
        },
        |(_, _, trim_where, trim_str, _, expr, _)| ExprElement::Trim {
            expr: Box::new(expr),
            trim_where: Some((trim_where, Box::new(trim_str))),
        },
    );
    let count_all = value(ExprElement::CountAll, rule! {
        COUNT ~ "(" ~ "*" ~ ^")"
    });
    let tuple = map(
        rule! {
            "(" ~ #subexpr(0) ~ "," ~ #comma_separated_list1_allow_trailling(subexpr(0))? ~ ","? ~ ^")"
        },
        |(_, head, _, opt_tail, _, _)| {
            let mut exprs = opt_tail.unwrap_or_default();
            exprs.insert(0, head);
            ExprElement::Tuple { exprs }
        },
    );
    let function_call = map(
        rule! {
            #function_name
            ~ "("
            ~ DISTINCT?
            ~ #comma_separated_list0(subexpr(0))?
            ~ ")"
        },
        |(name, _, opt_distinct, opt_args, _)| ExprElement::FunctionCall {
            distinct: opt_distinct.is_some(),
            name,
            args: opt_args.unwrap_or_default(),
            params: vec![],
        },
    );
    let function_call_with_param = map(
        rule! {
            #function_name
            ~ "(" ~ #comma_separated_list1(literal) ~ ")"
            ~ "(" ~ DISTINCT? ~ #comma_separated_list0(subexpr(0))? ~ ")"
        },
        |(name, _, params, _, _, opt_distinct, opt_args, _)| ExprElement::FunctionCall {
            distinct: opt_distinct.is_some(),
            name,
            args: opt_args.unwrap_or_default(),
            params,
        },
    );
    let case = map(
        rule! {
            CASE ~ #subexpr(0)?
            ~ ( WHEN ~ ^#subexpr(0) ~ ^THEN ~ ^#subexpr(0) )+
            ~ ( ELSE ~ ^#subexpr(0) )? ~ ^END
        },
        |(_, operand, branches, else_result, _)| {
            let (conditions, results) = branches
                .into_iter()
                .map(|(_, cond, _, result)| (cond, result))
                .unzip();
            let else_result = else_result.map(|(_, result)| result);
            ExprElement::Case {
                operand: operand.map(Box::new),
                conditions,
                results,
                else_result: else_result.map(Box::new),
            }
        },
    );
    let exists = map(
        rule! { EXISTS ~ ^"(" ~ ^#query ~ ^")" },
        |(_, _, subquery, _)| ExprElement::Exists { subquery },
    );
    let subquery = map(
        rule! {
            "("
            ~ #query
            ~ ^")"
        },
        |(_, subquery, _)| ExprElement::Subquery { subquery },
    );
    let group = map(
        rule! {
           "("
           ~ ^#subexpr(0)
           ~ ^")"
        },
        |(_, expr, _)| ExprElement::Group(expr),
    );
    let binary_op = map(binary_op, |op| ExprElement::BinaryOp { op });
    let unary_op = map(unary_op, |op| ExprElement::UnaryOp { op });
    let literal = map(literal, |lit| ExprElement::Literal { lit });
    let map_access = map(map_access, |accessor| ExprElement::MapAccess { accessor });
    let array = map(
        rule! {
            "[" ~ #comma_separated_list0(subexpr(0))? ~ ","? ~ ^"]"
        },
        |(_, opt_args, _, _)| {
            let exprs = opt_args.unwrap_or_default();
            ExprElement::Array { exprs }
        },
    );

    let (rest, (span, elem)) = consumed(alt((
        rule! (
            #is_null : "`... IS [NOT] NULL`"
            | #in_list : "`[NOT] IN (<expr>, ...)`"
            | #in_subquery : "`[NOT] IN (SELECT ...)`"
            | #between : "`[NOT] BETWEEN ... AND ...`"
            | #binary_op : "<operator>"
            | #unary_op : "<operator>"
            | #cast : "`CAST(... AS ...)`"
            | #pg_cast : "`::<type_name>`"
            | #extract : "`EXTRACT((YEAR | MONTH | DAY | HOUR | MINUTE | SECOND) FROM ...)`"
            | #position : "`POSITION(... IN ...)`"
            | #substring : "`SUBSTRING(... [FROM ...] [FOR ...])`"
            | #trim : "`TRIM(...)`"
            | #trim_from : "`TRIM([(BOTH | LEADEING | TRAILING) ... FROM ...)`"
        ),
        rule!(
            #count_all : "COUNT(*)"
            | #tuple : "`(<expr>, ...)`"
            | #function_call_with_param : "<function>"
            | #function_call : "<function>"
            | #literal : "<literal>"
            | #case : "`CASE ... END`"
            | #exists : "`EXISTS (SELECT ...)`"
            | #subquery : "`(SELECT ...)`"
            | #group
            | #column_ref : "<column>"
            | #map_access : "[<key>] | .<key> | :<key>"
            | #datetime_unit: "<unit>"
            | #array : "`[...]`"
        ),
    )))(i)?;

    Ok((rest, WithSpan { span, elem }))
}

pub fn unary_op(i: Input) -> IResult<UnaryOperator> {
    // Plus and Minus are parsed as binary op at first.
    value(UnaryOperator::Not, rule! { NOT })(i)
}

pub fn binary_op(i: Input) -> IResult<BinaryOperator> {
    alt((
        alt((
            value(BinaryOperator::Plus, rule! { "+" }),
            value(BinaryOperator::Minus, rule! { "-" }),
            value(BinaryOperator::Multiply, rule! { "*" }),
            value(BinaryOperator::Divide, rule! { "/" }),
            value(BinaryOperator::Div, rule! { DIV }),
            value(BinaryOperator::Modulo, rule! { "%" }),
            value(BinaryOperator::StringConcat, rule! { "||" }),
            value(BinaryOperator::Gt, rule! { ">" }),
            value(BinaryOperator::Lt, rule! { "<" }),
            value(BinaryOperator::Gte, rule! { ">=" }),
            value(BinaryOperator::Lte, rule! { "<=" }),
            value(BinaryOperator::Eq, rule! { "=" }),
            value(BinaryOperator::NotEq, rule! { "<>" | "!=" }),
        )),
        alt((
            value(BinaryOperator::And, rule! { AND }),
            value(BinaryOperator::Or, rule! { OR }),
            value(BinaryOperator::Xor, rule! { XOR }),
            value(BinaryOperator::Like, rule! { LIKE }),
            value(BinaryOperator::NotLike, rule! { NOT ~ LIKE }),
            value(BinaryOperator::Regexp, rule! { REGEXP }),
            value(BinaryOperator::NotRegexp, rule! { NOT ~ REGEXP }),
            value(BinaryOperator::RLike, rule! { RLIKE }),
            value(BinaryOperator::NotRLike, rule! { NOT ~ RLIKE }),
            value(BinaryOperator::BitwiseOr, rule! { "|" }),
            value(BinaryOperator::BitwiseAnd, rule! { "&" }),
            value(BinaryOperator::BitwiseXor, rule! { "^" }),
        )),
    ))(i)
}

pub fn literal(i: Input) -> IResult<Literal> {
    let string = map(literal_string, Literal::String);
    // TODO(andylokandy): handle hex numbers in parser
    let number = map(
        rule! {
            LiteralHex | LiteralNumber
        },
        |number| Literal::Number(number.text().to_string()),
    );
    let boolean = alt((
        value(Literal::Boolean(true), rule! { TRUE }),
        value(Literal::Boolean(false), rule! { FALSE }),
    ));
    let interval = map(
        rule! {
            INTERVAL ~ #literal_string ~ #interval_kind
        },
        |(_, value, field)| Literal::Interval(Interval { value, kind: field }),
    );
    let current_timestamp = value(Literal::CurrentTimestamp, rule! { CURRENT_TIMESTAMP });
    let null = value(Literal::Null, rule! { NULL });

    rule!(
        #string
        | #number
        | #boolean
        | #interval : "`INTERVAL '...' (YEAR | MONTH | DAY | ...)`"
        | #current_timestamp
        | #null
    )(i)
}

pub fn type_name(i: Input) -> IResult<TypeName> {
    let ty_boolean = value(TypeName::Boolean, rule! { BOOLEAN | BOOL });
    let ty_uint8 = value(
        TypeName::UInt8,
        rule! { ( UINT8 | #map(rule! { TINYINT ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )?  },
    );
    let ty_uint16 = value(
        TypeName::UInt16,
        rule! { ( UINT16 | #map(rule! { SMALLINT ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_uint32 = value(
        TypeName::UInt32,
        rule! { ( UINT32 | #map(rule! { ( INT | INTEGER ) ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_uint64 = value(
        TypeName::UInt64,
        rule! { ( UINT64 | UNSIGNED | #map(rule! { BIGINT ~ UNSIGNED }, |(t, _)| t) ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int8 = value(
        TypeName::Int8,
        rule! { ( INT8 | TINYINT ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int16 = value(
        TypeName::Int16,
        rule! { ( INT16 | SMALLINT ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int32 = value(
        TypeName::Int32,
        rule! { ( INT32 | INT | INTEGER ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_int64 = value(
        TypeName::Int64,
        rule! { ( INT64 | SIGNED | BIGINT ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_float32 = value(TypeName::Float32, rule! { FLOAT32 | FLOAT });
    let ty_float64 = value(TypeName::Float64, rule! { FLOAT64 | DOUBLE });
    let ty_array = map(
        rule! { ARRAY ~ ( "(" ~ #type_name ~ ")" )? },
        |(_, opt_item_type)| TypeName::Array {
            item_type: opt_item_type.map(|(_, opt_item_type, _)| Box::new(opt_item_type)),
        },
    );
    let ty_date = value(TypeName::Date, rule! { DATE });
    let ty_datetime = map(
        rule! { DATETIME ~ ( "(" ~ #literal_u64 ~ ")" )? },
        |(_, opt_precision)| TypeName::DateTime {
            precision: opt_precision.map(|(_, precision, _)| precision),
        },
    );
    let ty_timestamp = value(TypeName::Timestamp, rule! { TIMESTAMP });
    let ty_string = value(
        TypeName::String,
        rule! { ( STRING | VARCHAR ) ~ ( "(" ~ #literal_u64 ~ ")" )? },
    );
    let ty_object = value(TypeName::Object, rule! { OBJECT | MAP });
    let ty_variant = value(TypeName::Variant, rule! { VARIANT | JSON });

    rule!(
        ( #ty_boolean
        | #ty_uint8
        | #ty_uint16
        | #ty_uint32
        | #ty_uint64
        | #ty_int8
        | #ty_int16
        | #ty_int32
        | #ty_int64
        | #ty_float32
        | #ty_float64
        | #ty_array
        | #ty_date
        | #ty_datetime
        | #ty_timestamp
        | #ty_string
        | #ty_object
        | #ty_variant
        ) : "type name"
    )(i)
}

pub fn interval_kind(i: Input) -> IResult<IntervalKind> {
    let year = value(IntervalKind::Year, rule! { YEAR });
    let month = value(IntervalKind::Month, rule! { MONTH });
    let day = value(IntervalKind::Day, rule! { DAY });
    let hour = value(IntervalKind::Hour, rule! { HOUR });
    let minute = value(IntervalKind::Minute, rule! { MINUTE });
    let second = value(IntervalKind::Second, rule! { SECOND });
    let doy = value(IntervalKind::Doy, rule! { DOY });
    let dow = value(IntervalKind::Dow, rule! { DOW });

    rule!(
        #year
        | #month
        | #day
        | #hour
        | #minute
        | #second
        | #doy
        | #dow
    )(i)
}

pub fn map_access(i: Input) -> IResult<MapAccessor> {
    let bracket = map(
        rule! {
           "[" ~ #literal ~ "]"
        },
        |(_, key, _)| MapAccessor::Bracket { key },
    );
    let bracket_ident = map(
        rule! {
           "[" ~ #ident ~ "]"
        },
        |(_, key, _)| MapAccessor::Bracket {
            key: Literal::String(key.name),
        },
    );
    let period = map(
        rule! {
           "." ~ #ident
        },
        |(_, key)| MapAccessor::Period { key },
    );
    let colon = map(
        rule! {
         ":" ~ #ident
        },
        |(_, key)| MapAccessor::Colon { key },
    );

    rule!(
        #bracket
        | #bracket_ident
        | #period
        | #colon
    )(i)
}
