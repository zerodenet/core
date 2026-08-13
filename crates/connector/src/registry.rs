use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use zero_api::EventSink;
use zero_config::{ApiConfig, EventSinkConfig};

use crate::state::PersistentStateLease;
use crate::{ConnectorError, ConnectorResult};

pub(crate) struct ConfiguredEventSink {
    pub(crate) tag: String,
    pub(crate) event_types: Vec<String>,
    pub(crate) source_id: Option<String>,
    sink: Arc<dyn AsyncDeliverySink>,
    _persistent_lease: Option<PersistentStateLease>,
}

impl ConfiguredEventSink {
    pub(crate) async fn publish_prepared(
        &self,
        event: &zero_api::RawApiEvent,
    ) -> zero_api::ApiResult<zero_api::PublishResult> {
        self.sink.publish(event.clone()).await
    }

    pub(crate) fn supports_cancellation(&self) -> bool {
        self.sink.supports_cancellation()
    }
}

/// Connector-local asynchronous delivery boundary.
///
/// `zero_api::EventSink` intentionally remains synchronous because the engine
/// also uses it for in-process durability hooks. Connector transports use this
/// narrower contract so network delivery can be cancelled without importing a
/// runtime into the API or engine crates.
#[async_trait]
pub(crate) trait AsyncDeliverySink: Send + Sync {
    async fn publish(
        &self,
        event: zero_api::RawApiEvent,
    ) -> zero_api::ApiResult<zero_api::PublishResult>;

    fn supports_cancellation(&self) -> bool {
        false
    }
}

struct BlockingEventSink {
    sink: Arc<dyn EventSink + Send + Sync>,
}

#[async_trait]
impl AsyncDeliverySink for BlockingEventSink {
    async fn publish(
        &self,
        event: zero_api::RawApiEvent,
    ) -> zero_api::ApiResult<zero_api::PublishResult> {
        let sink = self.sink.clone();
        tokio::task::spawn_blocking(move || sink.publish(&event))
            .await
            .map_err(|error| {
                zero_api::ApiError::new(
                    zero_api::ApiErrorCode::Internal,
                    format!("blocking event sink task failed: {error}"),
                )
            })?
    }
}

pub(crate) fn build_event_sinks(
    api: &ApiConfig,
    source_dir: Option<&Path>,
) -> ConnectorResult<Vec<ConfiguredEventSink>> {
    api.event_sinks
        .iter()
        .map(|config| {
            build_event_sink(
                config,
                source_dir,
                std::time::Duration::from_millis(api.dispatcher.webhook_timeout_ms),
            )
        })
        .collect()
}

fn build_event_sink(
    config: &EventSinkConfig,
    source_dir: Option<&Path>,
    webhook_timeout: std::time::Duration,
) -> ConnectorResult<ConfiguredEventSink> {
    match config {
        EventSinkConfig::JsonLines {
            tag,
            path,
            events,
            source_id,
        } => build_json_line_sink(tag, path, events, source_id, source_dir),
        EventSinkConfig::Webhook {
            tag,
            url,
            events,
            source_id,
            headers,
            allow_insecure,
        } => build_webhook_sink(
            tag,
            url,
            events,
            source_id,
            headers,
            *allow_insecure,
            webhook_timeout,
        ),
    }
}

#[cfg(feature = "sink-jsonl")]
fn build_json_line_sink(
    tag: &str,
    path: &str,
    events: &[String],
    source_id: &Option<String>,
    source_dir: Option<&Path>,
) -> ConnectorResult<ConfiguredEventSink> {
    use std::fs::OpenOptions;

    let resolved = resolve_path(path, source_dir);
    let persistent_lease = PersistentStateLease::acquire(&resolved)?;
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&resolved)
        .map_err(|source| ConnectorError::OpenJsonLineSink {
            tag: tag.to_owned(),
            path: resolved.display().to_string(),
            source,
        })?;

    Ok(ConfiguredEventSink {
        tag: tag.to_owned(),
        event_types: events.to_vec(),
        source_id: source_id.clone(),
        sink: Arc::new(BlockingEventSink {
            sink: Arc::new(zero_api::JsonLineEventSink::new(file)),
        }),
        _persistent_lease: Some(persistent_lease),
    })
}

#[cfg(not(feature = "sink-jsonl"))]
fn build_json_line_sink(
    tag: &str,
    _path: &str,
    _events: &[String],
    _source_id: &Option<String>,
    _source_dir: Option<&Path>,
) -> ConnectorResult<ConfiguredEventSink> {
    Err(ConnectorError::FeatureDisabled {
        feature: "sink-jsonl",
        sink_type: "jsonl",
        tag: tag.to_owned(),
    })
}

#[cfg(feature = "webhook")]
fn build_webhook_sink(
    tag: &str,
    url: &str,
    events: &[String],
    source_id: &Option<String>,
    headers: &std::collections::BTreeMap<String, String>,
    allow_insecure: bool,
    timeout: std::time::Duration,
) -> ConnectorResult<ConfiguredEventSink> {
    let mut config =
        crate::webhook::WebhookEventSinkConfig::new(url.to_owned()).with_timeout(timeout);
    for (name, value) in headers {
        config = config.with_header(name.clone(), value.clone());
    }
    if allow_insecure {
        config = config.with_allow_insecure(true);
    }
    let sink = crate::webhook::WebhookEventSink::with_config(config)?;

    Ok(ConfiguredEventSink {
        tag: tag.to_owned(),
        event_types: events.to_vec(),
        source_id: source_id.clone(),
        sink: Arc::new(sink),
        _persistent_lease: None,
    })
}

#[cfg(not(feature = "webhook"))]
fn build_webhook_sink(
    tag: &str,
    _url: &str,
    _events: &[String],
    _source_id: &Option<String>,
    _headers: &std::collections::BTreeMap<String, String>,
    _allow_insecure: bool,
    _timeout: std::time::Duration,
) -> ConnectorResult<ConfiguredEventSink> {
    Err(ConnectorError::FeatureDisabled {
        feature: "connector",
        sink_type: "webhook",
        tag: tag.to_owned(),
    })
}

pub(crate) fn resolve_path(path: &str, source_dir: Option<&Path>) -> PathBuf {
    let path = PathBuf::from(path);
    if path.is_absolute() {
        path
    } else if let Some(source_dir) = source_dir {
        source_dir.join(path)
    } else {
        path
    }
}
