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

use std::collections::VecDeque;

use crate::ast::write_comma_separated_list;
use crate::ast::Expr;
use crate::ast::MapAccessor;
use crate::ast::UnaryOperator;

pub trait Accept: Sized {
    type V: 'static;
    type Data;

    type Error;

    fn accept(&self, visitor: &mut Visitor<Self>, v: &Self::V) -> Result<(), Self::Error>;
}

enum StackFrame<V: 'static, Data, Err> {
    Action(Box<dyn FnOnce(&mut Data) -> Result<(), Err> + Send + Sync + 'static>),
    Children(&'static V),
}

pub struct Visitor<T: Accept> {
    data: T::Data,
    frame: VecDeque<StackFrame<T::V, T::Data, T::Error>>,
}

impl<T: Accept> Visitor<T> {
    pub fn visit_children(&mut self, v: &T::V) {
        self.frame.push_back(StackFrame::Children(unsafe { std::mem::transmute(v) }));
    }

    pub fn action<F: FnOnce(&mut T::Data) -> Result<(), T::Error> + Send + Sync>(&mut self, f: F) {
        let src: Box<dyn FnOnce(&mut T::Data) -> Result<(), T::Error> + Send + Sync> = Box::new(f);
        self.frame
            .push_back(StackFrame::Action(unsafe { std::mem::transmute(src) }));
    }

    pub fn visit(v: &T::V, data: T::Data, accept: T) -> Result<(), T::Error> {
        let mut executor = VisitorExecutor { accept };
        let visitor = Visitor {
            data,
            frame: Default::default(),
        };
        executor.visit(v, visitor)
    }
}

struct VisitorExecutor<T: Accept> {
    accept: T,
}

impl<T: Accept> VisitorExecutor<T> {
    pub fn visit(&mut self, v: &T::V, mut visitor: Visitor<T>) -> Result<(), T::Error> {
        self.accept.accept(&mut visitor, v)?;

        let frame = std::mem::take(&mut visitor.frame);
        let mut stack = vec![frame];

        while let Some(top) = stack.last_mut() {
            match top.pop_front() {
                None => {
                    stack.pop();
                }
                Some(frame) => match frame {
                    StackFrame::Action(action) => {
                        action(&mut visitor.data)?;
                    }
                    StackFrame::Children(children) => {
                        self.accept.accept(&mut visitor, children)?;
                        let frame = std::mem::take(&mut visitor.frame);
                        stack.push(frame);
                    }
                },
            }
        }

        Ok(())
    }
}

struct DisplayData {
    formatter: &'static mut std::fmt::Formatter<'static>,
}

struct DisplayExprAccept {}

impl Accept for DisplayExprAccept {
    type V = Expr;
    type Data = DisplayData;
    type Error = std::fmt::Error;

    fn accept(&self, visitor: &mut Visitor<Self>, v: &Expr) -> Result<(), Self::Error> {
        match v {
            Expr::ColumnRef { column, .. } => {
                visitor.action(move |data| {
                    if data.formatter.alternate() {
                        write!(data.formatter, "{column:#}")
                    } else {
                        write!(data.formatter, "{column}")
                    }
                });
            }
            Expr::IsNull { expr, not, .. } => {
                visitor.visit_children(expr);
                visitor.action(move |data| {
                    write!(data.formatter, " IS")?;

                    if *not {
                        write!(data.formatter, " NOT")?;
                    }
                    write!(data.formatter, " NULL")
                });
            }
            Expr::IsDistinctFrom {
                left, right, not, ..
            } => {
                visitor.visit_children(left);

                visitor.action(move |data| {
                    write!(data.formatter, " IS")?;
                    if *not {
                        write!(data.formatter, " NOT")?;
                    }

                    write!(data.formatter, " DISTINCT FROM ")
                });

                visitor.visit_children(right);
            }

            Expr::InList {
                expr, list, not, ..
            } => {
                visitor.visit_children(expr);

                visitor.action(move |data| {
                    if *not {
                        write!(data.formatter, " NOT")?;
                    }
                    write!(data.formatter, " IN(")?;

                    // TODO; visit children
                    write_comma_separated_list(data.formatter, list)?;
                    write!(data.formatter, ")")
                });
            }
            Expr::InSubquery {
                expr,
                subquery,
                not,
                ..
            } => {
                visitor.visit_children(expr);

                visitor.action(move |data| {
                    if *not {
                        write!(data.formatter, " NOT")?;
                    }
                    write!(data.formatter, " IN({subquery})")
                });
            }
            Expr::Between {
                expr,
                low,
                high,
                not,
                ..
            } => {
                visitor.visit_children(expr);
                visitor.action(move |data| {
                    if *not {
                        write!(data.formatter, " NOT")?;
                    }

                    write!(data.formatter, " BETWEEN ")
                });

                visitor.visit_children(low);
                visitor.action(move |data| write!(data.formatter, " AND "));
                visitor.visit_children(high);
            }
            Expr::UnaryOp { op, expr, .. } => {
                match op {
                    // TODO (xieqijun) Maybe special attribute are provided to check whether the symbol is before or after.
                    UnaryOperator::Factorial => {
                        visitor.visit_children(expr);
                        visitor.action(move |data| write!(data.formatter, " {op}"));
                    }
                    _ => {
                        visitor.action(move |data| write!(data.formatter, "{op} "));
                        visitor.visit_children(expr);
                    }
                }
            }
            Expr::BinaryOp {
                op, left, right, ..
            } => {
                visitor.visit_children(left);
                visitor.action(move |data| write!(data.formatter, " {op} "));
                visitor.visit_children(right);
            }
            Expr::JsonOp {
                op, left, right, ..
            } => {
                visitor.visit_children(left);
                visitor.action(move |data| write!(data.formatter, " {op} "));
                visitor.visit_children(right);
            }
            Expr::Cast {
                expr,
                target_type,
                pg_style,
                ..
            } => {
                if *pg_style {
                    visitor.visit_children(expr);
                    visitor.action(move |data| write!(data.formatter, "::{target_type}"));
                } else {
                    visitor.action(move |data| write!(data.formatter, "CAST("));
                    visitor.visit_children(expr);
                    visitor.action(move |data| write!(data.formatter, " AS {target_type})"));
                }
            }
            Expr::TryCast {
                expr, target_type, ..
            } => {
                visitor.action(move |data| write!(data.formatter, "TRY_CAST("));
                visitor.visit_children(expr);
                visitor.action(move |data| write!(data.formatter, " AS {target_type})"));
            }
            Expr::Extract {
                kind: field, expr, ..
            } => {
                visitor.action(move |data| write!(data.formatter, "EXTRACT({field} FROM "));
                visitor.visit_children(expr);
                visitor.action(move |data| write!(data.formatter, ")"));
            }
            Expr::DatePart {
                kind: field, expr, ..
            } => {
                visitor.action(move |data| write!(data.formatter, "DATE_PART({field}, "));
                visitor.visit_children(expr);
                visitor.action(move |data| write!(data.formatter, ")"));
            }
            Expr::Position {
                substr_expr,
                str_expr,
                ..
            } => {
                visitor.action(move |data| write!(data.formatter, "POSITION("));
                visitor.visit_children(substr_expr);
                visitor.action(move |data| write!(data.formatter, " IN "));
                visitor.visit_children(str_expr);
                visitor.action(move |data| write!(data.formatter, " )"));
            }
            Expr::Substring {
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                visitor.action(move |data| write!(data.formatter, "SUBSTRING("));
                visitor.visit_children(expr);
                visitor.action(move |data| write!(data.formatter, " FROM "));
                visitor.visit_children(substring_from);

                if let Some(substring_for) = substring_for {
                    visitor.action(move |data| write!(data.formatter, " FOR "));
                    visitor.visit_children(substring_for);
                }

                visitor.action(move |data| write!(data.formatter, ")"));
            }
            Expr::Trim {
                expr, trim_where, ..
            } => {
                visitor.action(move |data| write!(data.formatter, "TRIM("));

                if let Some((trim_where, trim_str)) = trim_where {
                    visitor.action(move |data| write!(data.formatter, "{trim_where} "));
                    visitor.visit_children(trim_str);
                    visitor.action(move |data| write!(data.formatter, " FROM "));
                }

                visitor.visit_children(expr);
                visitor.action(move |data| write!(data.formatter, ")"));
            }
            Expr::Literal { value, .. } => {
                visitor.action(move |data| write!(data.formatter, "{value}"));
            }
            Expr::CountAll { window, .. } => {
                visitor.action(move |data| write!(data.formatter, "COUNT(*)"));

                if let Some(window) = window {
                    visitor.action(move |data| write!(data.formatter, " OVER {window}"));
                }
            }
            Expr::Tuple { exprs, .. } => {
                visitor.action(move |data| {
                    write!(data.formatter, "(")?;
                    // TODO: visit children
                    write_comma_separated_list(data.formatter, exprs)?;

                    if exprs.len() == 1 {
                        write!(data.formatter, ",")?;
                    }

                    write!(data.formatter, ")")
                });
            }
            Expr::FunctionCall { func, .. } => {
                visitor.action(move |data| write!(data.formatter, "{func}"));
            }
            Expr::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                visitor.action(move |data| write!(data.formatter, "CASE"));

                if let Some(op) = operand {
                    visitor.action(move |data| write!(data.formatter, " {op} "));
                }

                for (cond, res) in conditions.iter().zip(results) {
                    visitor.action(move |data| write!(data.formatter, " WHEN "));
                    visitor.visit_children(cond);
                    visitor.action(move |data| write!(data.formatter, " THEN "));
                    visitor.visit_children(res);
                }

                if let Some(el) = else_result {
                    visitor.action(move |data| write!(data.formatter, " ELSE "));
                    visitor.visit_children(el);
                }

                visitor.action(move |data| write!(data.formatter, " END"));
            }
            Expr::Exists { not, subquery, .. } => {
                if *not {
                    visitor.action(move |data| write!(data.formatter, "NOT "));
                }

                visitor.action(move |data| write!(data.formatter, "EXISTS ({subquery})"));
            }
            Expr::Subquery {
                subquery, modifier, ..
            } => {
                if let Some(m) = modifier {
                    visitor.action(move |data| write!(data.formatter, "{m} "));
                }

                visitor.action(move |data| write!(data.formatter, "({subquery})"));
            }
            Expr::MapAccess { expr, accessor, .. } => {
                visitor.visit_children(expr);
                visitor.action(move |data| match accessor {
                    MapAccessor::Bracket { key } => write!(data.formatter, "[{key}]"),
                    MapAccessor::DotNumber { key } => write!(data.formatter, ".{key}"),
                    MapAccessor::Colon { key } => write!(data.formatter, ":{key}"),
                });
            }
            Expr::Array { exprs, .. } => {
                visitor.action(move |data| {
                    write!(data.formatter, "[")?;
                    write_comma_separated_list(data.formatter, exprs)?;
                    write!(data.formatter, "]")
                });
            }
            Expr::Map { kvs, .. } => {
                visitor.action(move |data| write!(data.formatter, "{{"));
                for (i, (k, v)) in kvs.iter().enumerate() {
                    if i > 0 {
                        visitor.action(move |data| write!(data.formatter, "."));
                    }

                    visitor.action(move |data| write!(data.formatter, "{k}:{v}"));
                }

                visitor.action(move |data| write!(data.formatter, "}}"));
            }
            Expr::Interval { expr, unit, .. } => {
                visitor.action(move |data| write!(data.formatter, "INTERVAL "));
                visitor.visit_children(expr);
                visitor.action(move |data| write!(data.formatter, " {unit}"));
            }
            Expr::DateAdd {
                unit,
                interval,
                date,
                ..
            } => {
                visitor.action(move |data| write!(data.formatter, "DATE_ADD({unit}, "));
                visitor.visit_children(interval);
                visitor.action(move |data| write!(data.formatter, ", "));
                visitor.visit_children(date);
                visitor.action(move |data| write!(data.formatter, ")"));
            }
            Expr::DateSub {
                unit,
                interval,
                date,
                ..
            } => {
                visitor.action(move |data| write!(data.formatter, "DATE_SUB({unit}, "));
                visitor.visit_children(interval);
                visitor.action(move |data| write!(data.formatter, ", "));
                visitor.visit_children(date);
                visitor.action(move |data| write!(data.formatter, ")"));
            }
            Expr::DateTrunc { unit, date, .. } => {
                visitor.action(move |data| write!(data.formatter, "DATE_TRUNC({unit}, "));
                visitor.visit_children(date);
                visitor.action(move |data| write!(data.formatter, ")"));
            }
            Expr::Hole { name, .. } => {
                visitor.action(move |data| write!(data.formatter, ":{name}"));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fmt::Display;
    use std::fmt::Formatter;

    use crate::ast::expr_visitor::DisplayData;
    use crate::ast::expr_visitor::DisplayExprAccept;
    use crate::ast::expr_visitor::Visitor;
    use crate::ast::BinaryOperator;
    use crate::ast::Expr;
    use crate::ast::Literal;

    #[test]
    fn test_display_expr_accept() {
        let mut expr = Expr::Literal {
            span: None,
            value: Literal::Boolean(true),
        };

        for _index in 0..5000 {
            expr = Expr::BinaryOp {
                span: None,
                op: BinaryOperator::Or,
                left: Box::new(expr),
                right: Box::new(Expr::Literal {
                    span: None,
                    value: Literal::Boolean(true),
                }),
            }
        }

        struct TestWarp(Expr);

        impl Display for TestWarp {
            fn fmt(&'_ self, f: &mut Formatter<'_>) -> std::fmt::Result {
                let expr = &self.0;
                let formatter: &'static mut Formatter<'static> = unsafe { std::mem::transmute(f) };
                let display_data = DisplayData { formatter };
                let expr_accept = DisplayExprAccept {};
                Visitor::visit(expr, display_data, expr_accept)
            }
        }

        println!("{}", TestWarp(expr));
    }
}
