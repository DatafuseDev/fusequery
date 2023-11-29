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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserSettingValue;
use dashmap::DashMap;
use itertools::Itertools;

use crate::settings_default::DefaultSettingValue;
use crate::settings_default::DefaultSettings;
use crate::SettingMode;

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub enum ScopeLevel {
    Global,
    Session,
}

impl Debug for ScopeLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            ScopeLevel::Global => {
                write!(f, "GLOBAL")
            }
            ScopeLevel::Session => {
                write!(f, "SESSION")
            }
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct ChangeValue {
    pub level: ScopeLevel,
    pub value: UserSettingValue,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Settings {
    pub(crate) tenant: String,
    pub(crate) changes: DashMap<String, ChangeValue>,
}

impl Settings {
    pub fn create(tenant: String) -> Arc<Settings> {
        Arc::new(Settings {
            tenant,
            changes: DashMap::new(),
        })
    }

    pub fn has_setting(&self, key: &str) -> Result<bool> {
        Ok(DefaultSettings::instance()?.settings.contains_key(key))
    }

    pub fn check_and_get_default_value(&self, key: &str) -> Result<UserSettingValue> {
        match DefaultSettings::instance()?.settings.get(key) {
            Some(v) => Ok(v.value.clone()),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }

    pub fn unset_setting(&self, k: &str) {
        self.changes.remove(k);
    }

    pub fn set_batch_settings(&self, settings: &HashMap<String, String>) -> Result<()> {
        for (k, v) in settings.iter() {
            if self.has_setting(k.as_str())? {
                self.set_setting(k.to_string(), v.to_string())?;
            }
        }

        Ok(())
    }

    pub fn is_changed(&self) -> bool {
        !self.changes.is_empty()
    }

    /// # Safety
    ///
    /// We will not validate the setting value type
    pub unsafe fn unchecked_apply_changes(&self, changes: &Settings) {
        for change in changes.changes.iter() {
            self.changes
                .insert(change.key().clone(), change.value().clone());
        }
    }
}

pub struct SettingsItem {
    pub name: String,
    pub level: ScopeLevel,
    pub desc: &'static str,
    pub user_value: UserSettingValue,
    pub default_value: UserSettingValue,
    pub possible_values: Option<Vec<&'static str>>,
}

pub struct SettingsIter<'a> {
    settings: &'a Settings,
    inner: std::vec::IntoIter<(String, DefaultSettingValue)>,
}

impl<'a> SettingsIter<'a> {
    pub fn create(settings: &'a Settings) -> SettingsIter<'a> {
        let iter = DefaultSettings::instance()
            .unwrap()
            .settings
            .clone()
            .into_iter()
            .sorted_by(|(l, _), (r, _)| Ord::cmp(l, r));

        SettingsIter::<'a> {
            settings,
            inner: iter,
        }
    }
}

impl<'a> Iterator for SettingsIter<'a> {
    type Item = SettingsItem;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            return match self.inner.next() {
                None => None,
                Some((_, value)) if matches!(value.mode, SettingMode::Write) => {
                    continue;
                }
                Some((key, default_value)) => Some(match self.settings.changes.get(&key) {
                    None => SettingsItem {
                        name: key,
                        level: ScopeLevel::Session,
                        desc: default_value.desc,
                        user_value: default_value.value.clone(),
                        default_value: default_value.value,
                        possible_values: default_value.possible_values,
                    },
                    Some(change_value) => SettingsItem {
                        name: key,
                        level: change_value.level.clone(),
                        desc: default_value.desc,
                        user_value: change_value.value.clone(),
                        default_value: default_value.value,
                        possible_values: default_value.possible_values,
                    },
                }),
            };
        }
    }
}

impl<'a> IntoIterator for &'a Settings {
    type Item = SettingsItem;
    type IntoIter = SettingsIter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        SettingsIter::<'a>::create(self)
    }
}
