//  Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use common_cache::Cache;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_fuse_meta::caches::CacheDeferMetrics;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::caches::ItemCache;
use common_fuse_meta::caches::TenantLabel;

/// Loads an object from a source
#[async_trait::async_trait]
pub trait Loader<T> {
    /// Loads object of type T, located at `location`
    async fn load(&self, location: &str, len_hint: Option<u64>, ver: u64) -> Result<T>;
}

pub trait HasTenantLabel {
    fn tenant_label(&self) -> TenantLabel;
}

/// A "cache-aware" reader
pub struct CachedReader<T, L> {
    cache: Option<ItemCache<T>>,
    loader: L,
    name: String,
}

impl<T, L> CachedReader<T, L>
where L: Loader<T> + HasTenantLabel
{
    pub fn new(cache: Option<ItemCache<T>>, loader: L, name: impl Into<String>) -> Self {
        Self {
            cache,
            loader,
            name: name.into(),
        }
    }

    /// Load the object at `location`, uses/populates the cache if possible/necessary.
    pub async fn read(
        &self,
        path: impl AsRef<str>,
        len_hint: Option<u64>,
        version: u64,
    ) -> Result<Arc<T>> {
        match &self.cache {
            None => self.load(path.as_ref(), len_hint, version).await,
            Some(cache) => {
                let tenant_label = self.loader.tenant_label();

                // in PR #3798, the cache is degenerated to metered by count of cached item,
                // later, when the size of BlockMeta could be acquired (needs some enhancements of crate `parquet2`)
                // 1) the `read_bytes` metric should be re-enabled
                // 2) the metrics need to be labeled by the name of cache as well

                let mut metrics = CacheDeferMetrics {
                    tenant_label,
                    cache_hit: false,
                    read_bytes: 0,
                };

                match self.get_by_cache(path.as_ref(), cache) {
                    Some(item) => {
                        metrics.cache_hit = true;
                        metrics.read_bytes = 0u64;
                        Ok(item)
                    }
                    None => {
                        let item = self.load(path.as_ref(), len_hint, version).await?;
                        let mut cache_guard = cache.write();
                        cache_guard.put(path.as_ref().to_owned(), item.clone());
                        Ok(item)
                    }
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        self.name.as_str()
    }

    fn get_by_cache(&self, key: &str, cache: &ItemCache<T>) -> Option<Arc<T>> {
        cache.write().get(key).cloned()
    }

    async fn load(&self, loc: &str, len_hint: Option<u64>, version: u64) -> Result<Arc<T>> {
        let val = self.loader.load(loc, len_hint, version).await?;
        let item = Arc::new(val);
        Ok(item)
    }
}

impl HasTenantLabel for &dyn TableContext {
    fn tenant_label(&self) -> TenantLabel {
        let storage_cache_manager = CacheManager::instance();
        TenantLabel {
            tenant_id: storage_cache_manager.get_tenant_id().to_owned(),
            cluster_id: storage_cache_manager.get_cluster_id().to_owned(),
        }
    }
}

impl HasTenantLabel for Arc<dyn TableContext> {
    fn tenant_label(&self) -> TenantLabel {
        self.as_ref().tenant_label()
    }
}
