//! Integration tests for the minimum export level and additional endpoints.

use logfire::config::{AdvancedOptions, OtlpEndpoint};
use opentelemetry::{
    KeyValue,
    logs::{LogRecord as _, Severity},
};
use opentelemetry_proto::tonic::collector::{
    logs::v1::ExportLogsServiceRequest, trace::v1::ExportTraceServiceRequest,
};
use prost::Message;
use tracing::Level;
use wiremock::matchers::{header, method};
use wiremock::{Mock, MockServer, ResponseTemplate};

use logfire::{configure, set_local_logfire};

/// The span names and log bodies a mock server received, and the attribute keys of each.
async fn received(server: &MockServer) -> (Vec<(String, Vec<String>)>, Vec<(String, Vec<String>)>) {
    let mut spans = Vec::new();
    let mut logs = Vec::new();
    for request in server.received_requests().await.unwrap() {
        match request.url.path() {
            "/v1/traces" => {
                let body = ExportTraceServiceRequest::decode(request.body.as_slice()).unwrap();
                for span in body
                    .resource_spans
                    .into_iter()
                    .flat_map(|r| r.scope_spans)
                    .flat_map(|s| s.spans)
                {
                    let is_pending = span.attributes.iter().any(|kv| {
                        kv.key == "logfire.span_type"
                            && format!("{:?}", kv.value).contains("pending_span")
                    });
                    if !is_pending {
                        let keys = span.attributes.into_iter().map(|kv| kv.key).collect();
                        spans.push((span.name, keys));
                    }
                }
            }
            "/v1/logs" => {
                let body = ExportLogsServiceRequest::decode(request.body.as_slice()).unwrap();
                for record in body
                    .resource_logs
                    .into_iter()
                    .flat_map(|r| r.scope_logs)
                    .flat_map(|s| s.log_records)
                {
                    let keys = record.attributes.into_iter().map(|kv| kv.key).collect();
                    logs.push((format!("{:?}", record.body.unwrap()), keys));
                }
            }
            _ => {}
        }
    }
    spans.sort();
    logs.sort();
    (spans, logs)
}

fn names(items: &[(String, Vec<String>)]) -> Vec<&str> {
    items.iter().map(|(name, _)| name.as_str()).collect()
}

#[tokio::test]
async fn test_min_export_level_and_additional_endpoint() {
    let logfire_server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&logfire_server)
        .await;
    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .and(header("authorization", "Bearer collector-token"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&collector)
        .await;

    let logfire = configure()
        .local()
        .send_to_logfire(true)
        .with_token("test-token")
        .with_default_level_filter(tracing::level_filters::LevelFilter::DEBUG)
        .with_advanced_options(
            AdvancedOptions::default()
                .with_base_url(logfire_server.uri())
                .with_min_export_level(Level::INFO)
                .with_additional_endpoint(
                    OtlpEndpoint::new(collector.uri())
                        .with_header("Authorization", "Bearer collector-token")
                        .with_span_filter(|span| {
                            span.attributes.push(KeyValue::new("export", "collector"));
                            span.name != "dropped_span"
                        })
                        .with_log_filter(|record| {
                            record.add_attribute("export", "collector");
                            record.severity_number() != Some(Severity::Warn)
                        }),
                ),
        )
        .finish()
        .unwrap();

    let guard = set_local_logfire(logfire);
    tracing::info_span!("info_span").in_scope(|| {
        tracing::debug_span!("debug_span").in_scope(|| {});
        tracing::info_span!("dropped_span").in_scope(|| {});
        tracing::debug!("debug log");
        tracing::info!("info log");
        tracing::warn!("warn log");
    });
    guard.shutdown().unwrap();

    let (spans, logs) = received(&logfire_server).await;
    assert_eq!(names(&spans), ["dropped_span", "info_span"]);
    assert_eq!(names(&logs).len(), 2, "{logs:?}");
    assert!(logs.iter().all(|(body, keys)| {
        !body.contains("debug log") && !keys.contains(&"export".to_string())
    }));
    assert!(
        spans
            .iter()
            .all(|(_, keys)| !keys.contains(&"export".to_string()))
    );

    let (spans, logs) = received(&collector).await;
    assert_eq!(names(&spans), ["debug_span", "info_span"]);
    assert_eq!(logs.len(), 2, "{logs:?}");
    assert!(logs.iter().all(|(body, _)| !body.contains("warn log")));
    assert!(
        spans
            .iter()
            .chain(&logs)
            .all(|(_, keys)| keys.contains(&"export".to_string()))
    );
}

#[tokio::test]
async fn test_additional_endpoint_without_logfire() {
    let collector = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&collector)
        .await;

    let logfire = configure()
        .local()
        .send_to_logfire(false)
        .with_advanced_options(
            AdvancedOptions::default().with_additional_endpoint(OtlpEndpoint::new(collector.uri())),
        )
        .finish()
        .unwrap();

    let guard = set_local_logfire(logfire);
    tracing::info_span!("info_span").in_scope(|| tracing::info!("info log"));
    guard.shutdown().unwrap();

    let (spans, logs) = received(&collector).await;
    assert_eq!(names(&spans), ["info_span"]);
    assert_eq!(logs.len(), 1, "{logs:?}");
}
