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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;

#[derive(Debug)]
pub struct Counter {
    value: Arc<AtomicU64>,
}

impl Clone for Counter {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
        }
    }
}

impl Default for Counter {
    fn default() -> Self {
        Counter {
            value: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl Counter {
    /// Increase the [`Counter`] by 1, returning the previous value.
    pub fn inc(&self) -> u64 {
        self.value.fetch_add(1, Ordering::Relaxed)
    }

    /// Increase the [`Counter`] by `v`, returning the previous value.
    pub fn inc_by(&self, v: u64) -> u64 {
        self.value.fetch_add(v, Ordering::Relaxed)
    }

    /// Get the current value of the [`Counter`].
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset the [`Counter`] to 0.
    pub fn reset(&self) {
        self.value.store(0, Ordering::Release)
    }
}

impl TypedMetric for Counter {
    const TYPE: MetricType = MetricType::Counter;
}

impl EncodeMetric for Counter {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        encoder.encode_counter::<(), _, u64>(&self.get(), None)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}
