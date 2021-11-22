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

use std::sync::Arc;

use common_exception::Result;

use crate::datasources::database::fuse::database::FuseDatabase;
use crate::datasources::database_engine_registry::DatabaseEngineRegistry;

#[test]
fn test_database_engine_registry() -> Result<()> {
    let registry = DatabaseEngineRegistry::default();
    registry.register("DEFAULT", Arc::new(FuseDatabase::try_create))?;

    let engine = registry.get_database_factory("default");
    assert!(engine.is_some());

    let engine = registry.get_database_factory("xx");
    assert!(engine.is_none());

    Ok(())
}
