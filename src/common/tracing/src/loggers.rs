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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::BufWriter;
use std::time::Duration;
use std::time::SystemTime;

use opentelemetry::logs::AnyValue;
use opentelemetry::logs::Logger as _;
use opentelemetry::logs::Severity;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::logs::Logger;
use tracing_appender::non_blocking::NonBlocking;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::RollingFileAppender;
use tracing_appender::rolling::Rotation;

/// Create a `BufWriter<NonBlocking>` for a rolling file logger.
///
/// `BufWriter` collects log segments into a whole before sending to underlying writer.
/// `NonBlocking` sends log to another thread to execute the write IO to avoid blocking the thread
/// that calls `log`.
///
/// Note that `NonBlocking` will discard logs if there are too many `io::Write::write(NonBlocking)`,
/// especially when `fern` sends log segments one by one to the `Writer`.
/// Therefore a `BufWriter` is used to reduce the number of `io::Write::write(NonBlocking)`.
pub(crate) fn new_file_log_writer(
    dir: &str,
    name: impl ToString,
) -> (BufWriter<NonBlocking>, WorkerGuard) {
    let rolling = RollingFileAppender::new(Rotation::HOURLY, dir, name.to_string());
    let (non_blocking, flush_guard) = tracing_appender::non_blocking(rolling);
    let buffered_non_blocking = BufWriter::with_capacity(64 * 1024 * 1024, non_blocking);

    (buffered_non_blocking, flush_guard)
}

pub(crate) struct MinitraceLogger;

impl log::Log for MinitraceLogger {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        if record.key_values().count() == 0 {
            minitrace::Event::add_to_local_parent(record.level().as_str(), || {
                [("message".into(), format!("{}", record.args()).into())]
            });
        } else {
            minitrace::Event::add_to_local_parent(record.level().as_str(), || {
                let mut pairs = Vec::with_capacity(record.key_values().count() + 1);
                pairs.push(("message".into(), format!("{}", record.args()).into()));
                let mut visitor = KvCollector { fields: &mut pairs };
                record.key_values().visit(&mut visitor).ok();
                pairs
            });
        }

        struct KvCollector<'a> {
            fields: &'a mut Vec<(Cow<'static, str>, Cow<'static, str>)>,
        }

        impl<'a, 'kvs> log::kv::Visitor<'kvs> for KvCollector<'a> {
            fn visit_pair(
                &mut self,
                key: log::kv::Key<'kvs>,
                value: log::kv::Value<'kvs>,
            ) -> Result<(), log::kv::Error> {
                self.fields
                    .push((key.as_str().to_string().into(), value.to_string().into()));
                Ok(())
            }
        }
    }

    fn flush(&self) {}
}

pub(crate) struct OpenTelemetryOTLPLogWriter {
    logger: Logger,
}

pub(crate) fn new_otlp_log_writer(
    endpoint: &str,
    labels: BTreeMap<String, String>,
) -> OpenTelemetryOTLPLogWriter {
    let kvs = labels
        .into_iter()
        .map(|(k, v)| opentelemetry::KeyValue::new(k, v))
        .collect::<Vec<_>>();
    let log_config = opentelemetry_sdk::logs::Config {
        resource: Cow::Owned(opentelemetry::sdk::Resource::new(kvs)),
    };
    let export_config = opentelemetry_otlp::ExportConfig {
        endpoint: endpoint.to_string(),
        protocol: opentelemetry_otlp::Protocol::Grpc,
        timeout: Duration::from_secs(opentelemetry_otlp::OTEL_EXPORTER_OTLP_TIMEOUT_DEFAULT),
    };
    let exporter = opentelemetry_otlp::new_exporter()
        .tonic()
        .with_export_config(export_config);
    let logger = opentelemetry_otlp::new_pipeline()
        .logging()
        .with_exporter(exporter)
        .with_log_config(log_config)
        .install_batch(opentelemetry_sdk::runtime::Tokio)
        .expect("install query log otlp pipeline");
    OpenTelemetryOTLPLogWriter { logger }
}

impl log::Log for OpenTelemetryOTLPLogWriter {
    fn enabled(&self, _metadata: &log::Metadata<'_>) -> bool {
        // we handle level and target filter with fern
        true
    }

    fn log(&self, record: &log::Record<'_>) {
        let builder = opentelemetry::logs::LogRecord::builder()
            .with_observed_timestamp(SystemTime::now())
            .with_severity_number(map_severity_to_otel_severity(record.level()))
            .with_severity_text(record.level().as_str())
            .with_body(AnyValue::from(record.args().to_string()));
        self.logger.emit(builder.build())
    }

    fn flush(&self) {
        match self.logger.provider() {
            Some(provider) => {
                let result = provider.force_flush();
                for r in result {
                    if let Err(e) = r {
                        eprintln!("flush log failed: {}", e);
                    }
                }
            }
            None => {
                eprintln!("flush log failed: logger provider is None");
            }
        }
    }
}

fn map_severity_to_otel_severity(level: log::Level) -> Severity {
    match level {
        log::Level::Error => Severity::Error,
        log::Level::Warn => Severity::Warn,
        log::Level::Info => Severity::Info,
        log::Level::Debug => Severity::Debug,
        log::Level::Trace => Severity::Trace,
    }
}
