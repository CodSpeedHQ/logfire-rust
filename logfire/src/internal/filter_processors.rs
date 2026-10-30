use std::{sync::Arc, time::Duration};

use opentelemetry::{InstrumentationScope, Value, logs::Severity};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    logs::{LogProcessor, SdkLogRecord},
    trace::{Span, SpanData, SpanProcessor},
};

pub(crate) type SpanFilter = Arc<dyn Fn(&mut SpanData) -> bool + Send + Sync>;
pub(crate) type LogFilter = Arc<dyn Fn(&mut SdkLogRecord) -> bool + Send + Sync>;

/// Wrapper around a `SpanProcessor` which only passes on the spans at `min_level` or above (by
/// their `logfire.level_num`, spans without one pass) that `filter` keeps, as `filter` left them.
pub(crate) struct FilterSpanProcessor<T> {
    inner: T,
    min_level: Option<Severity>,
    filter: Option<SpanFilter>,
}

impl<T> FilterSpanProcessor<T> {
    pub(crate) fn new(inner: T, min_level: Option<Severity>, filter: Option<SpanFilter>) -> Self {
        Self {
            inner,
            min_level,
            filter,
        }
    }
}

impl<T> std::fmt::Debug for FilterSpanProcessor<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterSpanProcessor")
            .field("min_level", &self.min_level)
            .finish_non_exhaustive()
    }
}

impl<T: SpanProcessor> SpanProcessor for FilterSpanProcessor<T> {
    fn on_start(&self, span: &mut Span, cx: &opentelemetry::Context) {
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, mut span: SpanData) {
        if let Some(min_level) = self.min_level {
            let level = span
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == "logfire.level_num");
            if let Some(kv) = level
                && let Value::I64(level) = kv.value
                && level < min_level as i64
            {
                return;
            }
        }
        if let Some(filter) = &self.filter
            && !filter(&mut span)
        {
            return;
        }
        self.inner.on_end(span);
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}

/// Wrapper around a `LogProcessor` which only passes on the records at `min_level` or above
/// (records without a severity pass) that `filter` keeps, as `filter` left them.
///
/// The record is shared with the processors after this one, so `filter` gets a copy of it.
pub(crate) struct FilterLogProcessor<T> {
    inner: T,
    min_level: Option<Severity>,
    filter: Option<LogFilter>,
}

impl<T> FilterLogProcessor<T> {
    pub(crate) fn new(inner: T, min_level: Option<Severity>, filter: Option<LogFilter>) -> Self {
        Self {
            inner,
            min_level,
            filter,
        }
    }
}

impl<T> std::fmt::Debug for FilterLogProcessor<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilterLogProcessor")
            .field("min_level", &self.min_level)
            .finish_non_exhaustive()
    }
}

impl<T: LogProcessor> LogProcessor for FilterLogProcessor<T> {
    fn emit(&self, record: &mut SdkLogRecord, scope: &InstrumentationScope) {
        if let Some(min_level) = self.min_level
            && let Some(severity) = record.severity_number()
            && (severity as i32) < (min_level as i32)
        {
            return;
        }
        let Some(filter) = &self.filter else {
            self.inner.emit(record, scope);
            return;
        };
        let mut record = record.clone();
        if filter(&mut record) {
            self.inner.emit(&mut record, scope);
        }
    }

    fn force_flush(&self) -> OTelSdkResult {
        self.inner.force_flush()
    }

    fn shutdown_with_timeout(&self, timeout: Duration) -> OTelSdkResult {
        self.inner.shutdown_with_timeout(timeout)
    }

    fn set_resource(&mut self, resource: &Resource) {
        self.inner.set_resource(resource);
    }
}
