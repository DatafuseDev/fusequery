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

use std::env;
use std::sync::Once;

use opentelemetry::global;
use opentelemetry::sdk::propagation::TraceContextPropagator;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;
use tracing_bunyan_formatter::BunyanFormattingLayer;
use tracing_bunyan_formatter::JsonStorageLayer;
use tracing_subscriber::fmt;
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::prelude::*;
use tracing_subscriber::registry::Registry;
use tracing_subscriber::EnvFilter;

/// Write logs to stdout.
pub fn init_default_tracing() {
    static START: Once = Once::new();

    START.call_once(|| {
        init_tracing_stdout();
    });
}

/// Init logging and tracing.
///
/// To enable reporting tracing data to jaeger, set env var `FUSE_JAEGER` to non-empty value.
/// A local tracing collection(maybe for testing) can be done with a local jaeger server.
/// To report tracing data and view it:
///   docker run -d -p6831:6831/udp -p6832:6832/udp -p16686:16686 jaegertracing/all-in-one:latest
///   FUSE_JAEGER=on RUST_LOG=trace cargo test
///   open http://localhost:16686/
///
/// To adjust batch sending delay, use `OTEL_BSP_SCHEDULE_DELAY`:
///   FUSE_JAEGER=on RUST_LOG=trace OTEL_BSP_SCHEDULE_DELAY=1 cargo test
///
// TODO(xp): use FUSE_JAEGER to assign jaeger server address.
fn init_tracing_stdout() {
    let fmt_layer = Layer::default()
        .with_thread_ids(true)
        .with_thread_names(true)
        .pretty()
        .with_ansi(true)
        .with_span_events(fmt::format::FmtSpan::FULL);

    let fuse_jaeger = env::var("FUSE_JAEGER").unwrap_or_else(|_| "".to_string());
    let ot_layer = if !fuse_jaeger.is_empty() {
        global::set_text_map_propagator(TraceContextPropagator::new());

        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_service_name("datafuse-store")
            .install_batch(opentelemetry::runtime::Tokio)
            .expect("install");

        let ot_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        Some(ot_layer)
    } else {
        None
    };

    let subscriber = Registry::default()
        .with(EnvFilter::from_default_env())
        .with(fmt_layer)
        .with(ot_layer);

    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");
}

/// Write logs to file and rotation by HOUR.
pub fn init_tracing_with_file(app_name: &str, dir: &str, level: &str) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_logging_layer = Layer::new().with_writer(stdout_writer);
    guards.push(stdout_guard);

    let file_appender = RollingFileAppender::new(Rotation::HOURLY, dir, app_name);
    let (file_writer, file_guard) = tracing_appender::non_blocking(file_appender);
    let file_logging_layer = BunyanFormattingLayer::new(app_name.to_string(), file_writer);
    guards.push(file_guard);

    let subscriber = Registry::default()
        .with(EnvFilter::new(level))
        .with(stdout_logging_layer)
        .with(JsonStorageLayer)
        .with(file_logging_layer);
    tracing::subscriber::set_global_default(subscriber)
        .expect("error setting global tracing subscriber");

    guards
}
