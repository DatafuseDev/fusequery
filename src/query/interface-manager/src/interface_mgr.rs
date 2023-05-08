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

use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_base::base::GlobalInstance;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_storages_fuse::FuseTable;

/// All `ee` interface need to add a fn in `InterfaceManager` trait

#[async_trait::async_trait]
pub trait InterfaceManager: Sync + Send {
    async fn do_vacuum(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
    ) -> Result<()>;
}

pub struct InterfaceManagerWrapper {
    manager: Box<dyn InterfaceManager>,
}

unsafe impl Send for InterfaceManagerWrapper {}
unsafe impl Sync for InterfaceManagerWrapper {}

impl InterfaceManagerWrapper {
    pub fn new(manager: Box<dyn InterfaceManager>) -> Self {
        Self { manager }
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
    ) -> Result<()> {
        self.manager
            .do_vacuum(fuse_table, ctx, retention_time)
            .await
    }
}

pub fn get_interface_manager() -> Arc<InterfaceManagerWrapper> {
    GlobalInstance::get()
}
