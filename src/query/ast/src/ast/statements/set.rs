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

use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::Expr;
use crate::ast::Query;

// settings: set a = xxx
// variable: set variable a = xxx
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum SetType {
    #[Default]
    SettingsSession,
    SettingsGlobal,
    Variable,
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum SetValues {
    Expr(Vec<Box<Expr>>),
    Query(Box<Query>),
}
