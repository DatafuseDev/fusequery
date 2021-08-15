// Copyright 2020 Datafuse Labs.
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

#[allow(clippy::all)]
pub mod protobuf {
    // tonic::include_proto!("store_meta");
    include!(concat!(env!("OUT_DIR"), concat!("/store_meta.rs")));
}

#[cfg(test)]
#[macro_use]
pub mod tests;

pub mod api;
pub mod configs;
pub mod dfs;
pub mod executor;
pub mod fs;
pub mod localfs;
pub mod meta_service;
pub mod metrics;

mod data_part;
