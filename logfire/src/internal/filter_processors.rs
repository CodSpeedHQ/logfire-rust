use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use opentelemetry::{
    InstrumentationScope, KeyValue, SpanId, Value,
    logs::{LogRecord as _, Severity},
};
use opentelemetry_sdk::{
    Resource,
    error::OTelSdkResult,
    logs::{LogProcessor, SdkLogRecord},
    trace::{Span, SpanData, SpanProcessor},
};

pub(crate) type SpanFilter = Arc<dyn Fn(&mut SpanData) -> bool + Send + Sync>;
pub(crate) type LogFilter = Arc<dyn Fn(&mut SdkLogRecord) -> bool + Send + Sync>;

/// The open spans that the minimum level drops, each mapped to its nearest kept ancestor
/// (`SpanId::INVALID` when it has none).
///
/// The span and log processors of one export share it, so that the spans and logs under a
/// dropped span are moved onto that ancestor, and the export has no missing parents.
///
/// A dropped span is removed when it ends. A child that ends after it, such as a task spawned
/// with `.instrument`, keeps the dropped span as parent.
#[derive(Clone, Default)]
pub(crate) struct DroppedSpans(Arc<Mutex<HashMap<SpanId, SpanId>>>);

impl DroppedSpans {
    /// Returns the nearest kept ancestor of `span_id`, or `span_id` when it is kept.
    fn resolve(&self, span_id: SpanId) -> SpanId {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&span_id)
            .copied()
            .unwrap_or(span_id)
    }

    fn insert(&self, span_id: SpanId, ancestor: SpanId) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(span_id, ancestor);
    }

    fn remove(&self, span_id: SpanId) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&span_id);
    }
}

/// Returns true when the span's `logfire.level_num` is below `min_level`.
fn is_log_level_below(attributes: &[KeyValue], min_level: Severity) -> bool {
    attributes
        .iter()
        .find(|kv| kv.key.as_str() == "logfire.level_num")
        .is_some_and(|kv| matches!(kv.value, Value::I64(level) if level < min_level as i64))
}

/// Wrapper around a `SpanProcessor` which only passes on the spans at `min_level` or above (by
/// their `logfire.level_num`, spans without one pass) that `filter` keeps, as `filter` left them.
///
/// With a `min_level`, it records the spans it drops in `dropped`, and moves the kept spans
/// under a dropped span onto its nearest kept ancestor.
pub(crate) struct FilterSpanProcessor<T> {
    inner: T,
    min_level: Option<Severity>,
    filter: Option<SpanFilter>,
    dropped: DroppedSpans,
}

impl<T> FilterSpanProcessor<T> {
    pub(crate) fn new(
        inner: T,
        min_level: Option<Severity>,
        filter: Option<SpanFilter>,
        dropped: DroppedSpans,
    ) -> Self {
        Self {
            inner,
            min_level,
            filter,
            dropped,
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
        if let Some(min_level) = self.min_level
            && let Some(data) = span.exported_data()
            && is_log_level_below(&data.attributes, min_level)
        {
            self.dropped.insert(
                data.span_context.span_id(),
                self.dropped.resolve(data.parent_span_id),
            );
        }
        self.inner.on_start(span, cx);
    }

    fn on_end(&self, mut span: SpanData) {
        if let Some(min_level) = self.min_level {
            if is_log_level_below(&span.attributes, min_level) {
                self.dropped.remove(span.span_context.span_id());
                return;
            }
            span.parent_span_id = self.dropped.resolve(span.parent_span_id);

            // Only a pending span has this attribute: the parent of its real span. A pending span
            // comes here from `PendingSpanProcessor::on_start`, right after the real span's
            // `on_start`, so a dropped parent is still in `dropped`. Without a kept ancestor, the
            // real span is a root and the attribute is removed.
            if let Some(pending_parent_id) = span
                .attributes
                .iter_mut()
                .find(|kv| kv.key.as_str() == "logfire.pending_parent_id")
                && let Ok(parent) = SpanId::from_hex(pending_parent_id.value.as_str().as_ref())
            {
                let parent = self.dropped.resolve(parent);
                if parent == SpanId::INVALID {
                    span.attributes
                        .retain(|kv| kv.key.as_str() != "logfire.pending_parent_id");
                } else {
                    pending_parent_id.value = parent.to_string().into();
                }
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
///
/// A record in a span that `dropped` holds is passed on as a copy, moved onto the nearest kept
/// ancestor of that span.
pub(crate) struct FilterLogProcessor<T> {
    inner: T,
    min_level: Option<Severity>,
    filter: Option<LogFilter>,
    dropped: DroppedSpans,
}

impl<T> FilterLogProcessor<T> {
    pub(crate) fn new(
        inner: T,
        min_level: Option<Severity>,
        filter: Option<LogFilter>,
        dropped: DroppedSpans,
    ) -> Self {
        Self {
            inner,
            min_level,
            filter,
            dropped,
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
        let ancestor = record.trace_context().and_then(|context| {
            let ancestor = self.dropped.resolve(context.span_id);
            (ancestor != context.span_id).then_some((context.clone(), ancestor))
        });
        if self.filter.is_none() && ancestor.is_none() {
            self.inner.emit(record, scope);
            return;
        }
        let mut record = record.clone();
        if let Some((context, ancestor)) = ancestor {
            record.set_trace_context(context.trace_id, ancestor, context.trace_flags);
        }
        if self
            .filter
            .as_ref()
            .is_none_or(|filter| filter(&mut record))
        {
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use opentelemetry::trace::{SpanContext, SpanKind, Status, TraceFlags, TraceId, TraceState};
    use opentelemetry_sdk::trace::{SpanEvents, SpanLinks};

    use super::*;

    /// Keeps the spans it gets.
    #[derive(Debug, Default)]
    struct Collect(Mutex<Vec<SpanData>>);

    impl SpanProcessor for &Collect {
        fn on_start(&self, _span: &mut Span, _cx: &opentelemetry::Context) {}

        fn on_end(&self, span: SpanData) {
            self.0.lock().unwrap().push(span);
        }

        fn force_flush(&self) -> OTelSdkResult {
            Ok(())
        }

        fn shutdown_with_timeout(&self, _timeout: Duration) -> OTelSdkResult {
            Ok(())
        }
    }

    fn pending_span(parent: SpanId, pending_parent: SpanId) -> SpanData {
        SpanData {
            span_context: SpanContext::new(
                TraceId::from(1),
                SpanId::from(9),
                TraceFlags::SAMPLED,
                false,
                TraceState::default(),
            ),
            parent_span_id: parent,
            parent_span_is_remote: false,
            span_kind: SpanKind::Internal,
            name: Cow::Borrowed("pending"),
            start_time: std::time::SystemTime::UNIX_EPOCH,
            end_time: std::time::SystemTime::UNIX_EPOCH,
            attributes: vec![
                KeyValue::new("logfire.span_type", "pending_span"),
                KeyValue::new("logfire.pending_parent_id", pending_parent.to_string()),
            ],
            dropped_attributes_count: 0,
            events: SpanEvents::default(),
            links: SpanLinks::default(),
            status: Status::Unset,
            instrumentation_scope: InstrumentationScope::default(),
        }
    }

    fn pending_parent_id(span: &SpanData) -> Option<String> {
        span.attributes
            .iter()
            .find(|kv| kv.key.as_str() == "logfire.pending_parent_id")
            .map(|kv| kv.value.as_str().into_owned())
    }

    #[test]
    fn pending_parent_id_moves_to_kept_ancestor() {
        let collect = Collect::default();
        let dropped = DroppedSpans::default();
        dropped.insert(SpanId::from(2), SpanId::from(1));
        dropped.insert(SpanId::from(3), SpanId::INVALID);
        let processor = FilterSpanProcessor::new(&collect, Some(Severity::Info), None, dropped);

        processor.on_end(pending_span(SpanId::from(5), SpanId::from(2)));
        processor.on_end(pending_span(SpanId::from(5), SpanId::from(3)));
        processor.on_end(pending_span(SpanId::from(5), SpanId::from(4)));

        let spans = collect.0.lock().unwrap();
        assert_eq!(
            spans.iter().map(pending_parent_id).collect::<Vec<_>>(),
            [
                Some(SpanId::from(1).to_string()),
                None,
                Some(SpanId::from(4).to_string()),
            ]
        );
        assert!(
            spans
                .iter()
                .all(|span| span.parent_span_id == SpanId::from(5))
        );
    }
}
