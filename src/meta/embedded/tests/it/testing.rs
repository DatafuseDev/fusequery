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

use std::collections::BTreeMap;
use std::sync::Once;

use databend_common_base::base::tokio;
use databend_common_tracing::closure_name;
use databend_common_tracing::init_logging;
use databend_common_tracing::Config;
use fastrace::prelude::*;

pub fn mem_meta_test_harness<F, Fut>(test: F)
where
    F: FnOnce() -> Fut + 'static,
    Fut: std::future::Future<Output = anyhow::Result<()>> + Send + 'static,
{
    setup_test();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(3)
        .enable_all()
        .build()
        .unwrap();
    let root = Span::root(closure_name::<F>(), SpanContext::random());
    let test = test().in_span(root);
    rt.block_on(test).unwrap();

    shutdown_test();
}

fn setup_test() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let mut config = Config::new_testing();
        config.file.level = "DEBUG".to_string();
        let guards = init_logging("embedded_meta_unittests", &config, BTreeMap::new());
        Box::leak(Box::new(guards));
    });
}

fn shutdown_test() {
    fastrace::flush();
}
