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

use pretty::RcDoc;

use super::expr::pretty_expr;
use super::query::pretty_query;
use super::query::pretty_table;
use crate::ast::format::syntax::inline_comma;
use crate::ast::format::syntax::interweave_comma;
use crate::ast::format::syntax::NEST_FACTOR;
use crate::ast::DeleteStmt;
use crate::ast::InsertSource;
use crate::ast::InsertStmt;
use crate::ast::UpdateExpr;
use crate::ast::UpdateStmt;

pub(crate) fn pretty_insert(insert_stmt: InsertStmt) -> RcDoc<'static> {
    RcDoc::text("INSERT")
        .append(RcDoc::space())
        .append(if insert_stmt.overwrite {
            RcDoc::text("OVERWRITE")
        } else {
            RcDoc::text("INTO")
        })
        .append(
            RcDoc::line()
                .nest(NEST_FACTOR)
                .append(if let Some(catalog) = insert_stmt.catalog {
                    RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(if let Some(database) = insert_stmt.database {
                    RcDoc::text(database.to_string()).append(RcDoc::text("."))
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text(insert_stmt.table.to_string()))
                .append(if !insert_stmt.columns.is_empty() {
                    RcDoc::space()
                        .append(RcDoc::text("("))
                        .append(inline_comma(
                            insert_stmt
                                .columns
                                .into_iter()
                                .map(|ident| RcDoc::text(ident.to_string())),
                        ))
                        .append(RcDoc::text(")"))
                } else {
                    RcDoc::nil()
                }),
        )
        .append(pretty_source(insert_stmt.source))
}

fn pretty_source(source: InsertSource) -> RcDoc<'static> {
    RcDoc::line().append(match source {
        InsertSource::Values { rows } => RcDoc::text("VALUES").append(
            RcDoc::line().nest(NEST_FACTOR).append(
                interweave_comma(rows.into_iter().map(|row| {
                    RcDoc::text("(")
                        .append(inline_comma(row.into_iter().map(pretty_expr)))
                        .append(RcDoc::text(")"))
                }))
                .nest(NEST_FACTOR)
                .group(),
            ),
        ),
        InsertSource::RawValues { rest_str, .. } => RcDoc::text("VALUES").append(
            RcDoc::line()
                .nest(NEST_FACTOR)
                .append(RcDoc::text(rest_str)),
        ),
        InsertSource::Select { query } => pretty_query(*query),
    })
}

pub(crate) fn pretty_delete(delete_stmt: DeleteStmt) -> RcDoc<'static> {
    RcDoc::text("DELETE FROM")
        .append(
            RcDoc::line()
                .nest(NEST_FACTOR)
                .append(pretty_table(delete_stmt.table)),
        )
        .append(if let Some(selection) = delete_stmt.selection {
            RcDoc::line().append(RcDoc::text("WHERE")).append(
                RcDoc::line()
                    .nest(NEST_FACTOR)
                    .append(pretty_expr(selection).nest(NEST_FACTOR).group()),
            )
        } else {
            RcDoc::nil()
        })
}

pub(crate) fn pretty_update(update_stmt: UpdateStmt) -> RcDoc<'static> {
    RcDoc::text("UPDATE")
        .append(
            RcDoc::line()
                .nest(NEST_FACTOR)
                .append(pretty_table(update_stmt.table)),
        )
        .append(RcDoc::line().append(RcDoc::text("SET")))
        .append(pretty_update_list(update_stmt.update_list))
        .append(if let Some(selection) = update_stmt.selection {
            RcDoc::line().append(RcDoc::text("WHERE")).append(
                RcDoc::line()
                    .nest(NEST_FACTOR)
                    .append(pretty_expr(selection).nest(NEST_FACTOR).group()),
            )
        } else {
            RcDoc::nil()
        })
}

fn pretty_update_list(update_list: Vec<UpdateExpr>) -> RcDoc<'static> {
    if update_list.len() > 1 {
        RcDoc::line()
    } else {
        RcDoc::space()
    }
    .nest(NEST_FACTOR)
    .append(
        interweave_comma(update_list.into_iter().map(|update_expr| {
            RcDoc::text(update_expr.name.to_string())
                .append(RcDoc::space())
                .append(RcDoc::text("="))
                .append(RcDoc::space())
                .append(pretty_expr(update_expr.expr))
        }))
        .nest(NEST_FACTOR)
        .group(),
    )
}
