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
use std::time::Duration;

use chrono::{DateTime};
use chrono::Utc;
use crate::principal::UserIdentity;
use crate::schema::TableStatistics;

#[derive(
serde::Serialize,
serde::Deserialize,
Clone,
Debug,
Default,
Eq,
PartialEq,
num_derive::FromPrimitive,
)]
pub enum BackgroundTaskState {
    #[default]
    STARTED = 0,
    DONE = 1,
    FAILED = 2,
}

impl Display for BackgroundTaskState {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BackgroundTaskState::STARTED => write!(f, "STARTED"),
            BackgroundTaskState::DONE => write!(f, "DONE"),
            BackgroundTaskState::FAILED => write!(f, "FAILED"),
        }
    }
}


// BackgroundTaskType
#[derive(
serde::Serialize,
serde::Deserialize,
Clone,
Debug,
Default,
Eq,
PartialEq,
num_derive::FromPrimitive,
)]
pub enum BackgroundTaskType {
    #[default]
    COMPACTION = 0,
    VACUUM  = 1,
}

impl Display for BackgroundTaskType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            BackgroundTaskType::COMPACTION => write!(f, "COMPACTION"),
            BackgroundTaskType::VACUUM => write!(f, "VACUUM"),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundTaskIdent {
    pub tenant: String,
    pub task_id: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundTaskId {
    pub id: u64,
}

impl Display for BackgroundTaskIdent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.task_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionStats {
    pub db_id: u64,
    pub table_id: u64,
    pub before_compaction_stats: Option<TableStatistics>,
    pub after_compaction_stats: Option<TableStatistics>,
    pub total_compaction_time: Option<Duration>,
}

impl Display for CompactionStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "db_id: {}, table_id: {}, before_compaction_stats: {:?}, after_compaction_stats: {:?}, total_compaction_time: {:?}",
            self.db_id,
            self.table_id,
            self.before_compaction_stats,
            self.after_compaction_stats,
            self.total_compaction_time,
        )
    }
}

// TODO(zhihanz) provide detailed vacuum stats
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct VacuumStats {
}

impl Display for VacuumStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "vacuum stats")
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct BackgroundTaskInfo {
    pub last_updated: Option<DateTime<Utc>>,
    pub task_type: BackgroundTaskType,
    pub task_state: BackgroundTaskState,
    pub message: String,
    pub compaction_task_stats: Option<CompactionStats>,
    pub vacuum_stats: Option<VacuumStats>,
    pub creator: Option<UserIdentity>,
    pub created_at: DateTime<Utc>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundTaskReq {
    pub task_name: BackgroundTaskIdent,
    pub task_info: BackgroundTaskInfo,
    pub expire_at: u64,
}

impl Display for UpdateBackgroundTaskReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "update_background_task({}, {}, {}, {}, {:?})",
            self.task_name, self.task_info.task_type, self.task_info.task_state, self.task_info.message, self.task_info.last_updated
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UpdateBackgroundTaskReply {
    pub id: u64,
    pub last_updated: DateTime<Utc>,
    pub expire_at: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundTaskReq {
    pub name: BackgroundTaskIdent,
}

impl Display for GetBackgroundTaskReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "get_background_task({})", self.name
        )
    }
}


#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetBackgroundTaskReply {
    pub task_info: Option<BackgroundTaskInfo>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct ListBackgroundTasksReq {
    pub tenant: String,
}

impl Display for ListBackgroundTasksReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f, "list_background_tasks({})", self.tenant
        )
    }
}

impl ListBackgroundTasksReq {
    pub fn new(tenant: impl Into<String>) -> ListBackgroundTasksReq {
        ListBackgroundTasksReq {
            tenant: tenant.into(),
        }
    }
}

mod kvapi_key_impl {
    use common_meta_kvapi::kvapi;
    use crate::background::background_task::{BackgroundTaskId, BackgroundTaskIdent};
    const PREFIX_BACKGROUND: &str = "__fd_background_task";
    const PREFIX_BACKGROUND_TASK_BY_ID: &str = "__fd_background_task_by_id";

    /// <prefix>/<tenant>/<background_task_ident> -> <id>
    impl kvapi::Key for BackgroundTaskIdent {
        const PREFIX: &'static str = PREFIX_BACKGROUND;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(&self.tenant)
                .push_str(&self.task_id)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let tenant = p.next_str()?;
            let id = p.next_str()?;
            p.done()?;

            Ok(BackgroundTaskIdent { tenant, task_id: id })
        }
    }

    impl kvapi::Key for BackgroundTaskId {
        const PREFIX: &'static str = PREFIX_BACKGROUND_TASK_BY_ID;

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_u64(self.id)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;

            let id = p.next_u64()?;
            p.done()?;

            Ok(BackgroundTaskId { id })
        }
    }
}