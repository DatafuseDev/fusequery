//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

pub(crate) use meta::*;
pub(crate) use statistics::BlockStats;
pub(crate) use table::FuseTable;

mod index;
mod io;
mod meta;
mod statistics;
mod table;
mod table_do_append;
mod table_do_read;
mod table_do_read_partitions;
mod table_do_truncate;

#[cfg(test)]
mod table_do_append_test;
#[cfg(test)]
mod table_do_truncate_test;
#[cfg(test)]
mod table_test;
#[cfg(test)]
mod table_test_fixture;
