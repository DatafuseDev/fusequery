// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

pub use common::flight_result_to_str;
pub use common::status_err;
pub use flight_token::FlightClaim;
pub use flight_token::FlightToken;
pub use query_client::QueryClient;
pub use query_do_action::QueryDoAction;
pub use query_do_get::ExecutePlanAction;
pub use query_do_get::QueryDoGet;
pub use store_client::BlockStream;
pub use store_client::StoreClient;
pub use store_do_action::CreateDatabaseAction;
pub use store_do_action::CreateDatabaseActionResult;
pub use store_do_action::CreateTableAction;
pub use store_do_action::CreateTableActionResult;
pub use store_do_action::DropDatabaseAction;
pub use store_do_action::DropDatabaseActionResult;
pub use store_do_action::DropTableAction;
pub use store_do_action::DropTableActionResult;
pub use store_do_action::GetTableAction;
pub use store_do_action::GetTableActionResult;
pub use store_do_action::ReadPlanAction;
pub use store_do_action::ReadPlanActionResult;
pub use store_do_action::StoreDoAction;
pub use store_do_action::StoreDoActionResult;
pub use store_do_get::StoreDoGet;
// TODO refine these
pub use store_do_put::get_do_put_meta;
pub use store_do_put::set_do_put_meta;
pub use store_do_put::AppendResult;

mod common;
mod flight_token;
mod query_client;
mod query_do_action;
mod query_do_get;
mod store_client;
mod store_do_action;
mod store_do_get;
pub mod store_do_put;

// ProtoBuf generated files.
#[allow(clippy::all)]
pub mod protobuf {
    tonic::include_proto!("queryflight");
    tonic::include_proto!("storeflight");
}

#[cfg(test)]
mod store_do_put_test;
