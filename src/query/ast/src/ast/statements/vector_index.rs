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

use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;

use crate::ast::Expr;
use crate::ast::Identifier;
use crate::parser::token::TokenKind;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateVectorIndexStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub index_type: TokenKind,
    pub column: Identifier,
    pub metric_type: TokenKind,
    pub paras: Vec<Expr>,
}

impl Display for CreateVectorIndexStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "CREATE INDEX ON {} USING {:?}({} {:?})",
            self.table, self.index_type, self.column, self.metric_type
        )
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropVectorIndexStmt {
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub table: Identifier,
    pub column: Identifier,
    pub metric: TokenKind,
}

impl Display for DropVectorIndexStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP Vector INDEX")?;
        if self.if_exists {
            write!(f, " IF EXISTS")?;
        }
        if let Some(catalog) = &self.catalog {
            write!(f, " {}.", catalog)?;
        }
        if let Some(database) = &self.database {
            write!(f, " {}.", database)?;
        }
        write!(f, " {}", self.table)?;
        write!(f, " {}", self.column)?;
        write!(f, " {:?}", self.metric)?;
        Ok(())
    }
}
