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

use common_exception::Result;
use common_metatypes::KVMeta;
use common_metatypes::MatchSeq;
pub use common_store_api::kv_api::MGetKVActionResult;
pub use common_store_api::kv_api::PrefixListReply;
pub use common_store_api::kv_api::UpsertKVActionResult;
pub use common_store_api::GetKVActionResult;
use common_store_api::KVApi;
use common_tracing::tracing;

use crate::action_declare;
use crate::RequestFor;
use crate::StoreClient;
use crate::StoreDoAction;

#[async_trait::async_trait]
impl KVApi for StoreClient {
    #[tracing::instrument(level = "debug", skip(self, value))]
    async fn upsert_kv(
        &mut self,
        key: &str,
        seq: MatchSeq,
        value: Option<Vec<u8>>,
        value_meta: Option<KVMeta>,
    ) -> Result<UpsertKVActionResult> {
        self.do_action(UpsertKVAction {
            key: key.to_string(),
            seq,
            value,
            value_meta,
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_kv(&mut self, key: &str) -> Result<GetKVActionResult> {
        self.do_action(GetKVAction {
            key: key.to_string(),
        })
        .await
    }

    #[tracing::instrument(level = "debug", skip(self, keys))]
    async fn mget_kv(&mut self, keys: &[String]) -> common_exception::Result<MGetKVActionResult> {
        let keys = keys.to_vec();
        //keys.iter().map(|k| k.to_string()).collect();
        self.do_action(MGetKVAction { keys }).await
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn prefix_list_kv(&mut self, prefix: &str) -> common_exception::Result<PrefixListReply> {
        self.do_action(PrefixListReq(prefix.to_string())).await
    }
}

// Let take this API for a reference of the implementations of a store API

// - GetKV

// We wrap the "request of getting a kv" up here as GetKVAction,
// Technically we can use `String` directly, but as we are ...
// provides that StoreDoAction::GetKV is typed as `:: String -> StoreAction`

// The return type of GetKVAction is `GetActionResult`, which is defined by the KVApi,
// we use it directly here, but we can also wrap it up if needed.
//

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct GetKVAction {
    pub key: String,
}

// Explicitly defined (the request / reply relation)
// this can be simplified by using macro (see code below)
impl RequestFor for GetKVAction {
    type Reply = GetKVActionResult;
}

// Explicitly defined the converter for StoreDoAction
// It's implementations' choice, that they gonna using enum StoreDoAction as wrapper.
// This can be simplified by using macro (see code below)
impl From<GetKVAction> for StoreDoAction {
    fn from(act: GetKVAction) -> Self {
        StoreDoAction::GetKV(act)
    }
}

// - MGetKV

// Again, impl chooses to wrap it up
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct MGetKVAction {
    pub keys: Vec<String>,
}

// here we use a macro to simplify the declarations
action_declare!(MGetKVAction, MGetKVActionResult, StoreDoAction::MGetKV);

// - prefix list
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct PrefixListReq(pub String);
action_declare!(PrefixListReq, PrefixListReply, StoreDoAction::PrefixListKV);

// === general-kv: upsert ===
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct UpsertKVAction {
    pub key: String,
    pub seq: MatchSeq,
    pub value: Option<Vec<u8>>,
    pub value_meta: Option<KVMeta>,
}

action_declare!(
    UpsertKVAction,
    UpsertKVActionResult,
    StoreDoAction::UpsertKV
);
