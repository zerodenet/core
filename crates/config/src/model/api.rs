use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::inbound::ListenConfig;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ApiConfig {
    /// Independently registered event delivery channels.
    ///
    /// A registration is identified by its tag and selected by event type. It
    /// is not bound to a node, proxy protocol, inbound, or remote resource.
    #[serde(default)]
    pub event_sinks: Vec<EventSinkConfig>,
    #[serde(default)]
    pub control: ControlApiConfig,
    /// Flow hooks executed in registration order.
    #[serde(default)]
    pub hooks: Vec<HookConfig>,
    /// Path to the dead-letter JSONL file used for non-retryable deliveries
    /// and by the explicit `dead_letter` exhaustion policy.
    #[serde(default)]
    pub dead_letter_path: Option<String>,
    /// Append-only delivery journal used to recover unacknowledged sink
    /// deliveries after a process restart.
    #[serde(default)]
    pub outbox_path: Option<String>,
    /// Bounded dispatcher working set. `0` means outbox-only delivery and
    /// requires `outbox_path`; additional durable deliveries remain indexed
    /// in the outbox and are paged into memory as slots become free.
    #[serde(default)]
    pub dispatcher: EventDispatcherConfig,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventDispatcherConfig {
    #[serde(default = "default_dispatcher_max_in_memory_deliveries")]
    pub max_in_memory_deliveries: usize,
    #[serde(default = "default_dispatcher_replay_batch_size")]
    pub replay_batch_size: usize,
    /// Maximum retries after the initial delivery attempt before
    /// `exhausted_delivery_policy` applies. `retry_forever` continues after
    /// this threshold.
    #[serde(default = "default_dispatcher_max_retry_attempts")]
    pub max_retry_attempts: u32,
    #[serde(default = "default_dispatcher_retry_initial_delay_ms")]
    pub retry_initial_delay_ms: u64,
    #[serde(default = "default_dispatcher_retry_max_delay_ms")]
    pub retry_max_delay_ms: u64,
    #[serde(default = "default_webhook_timeout_ms")]
    pub webhook_timeout_ms: u64,
    /// Absolute free-space reserve kept on the filesystem containing the
    /// outbox. New delivery records pause before crossing this watermark.
    #[serde(default = "default_outbox_min_free_bytes")]
    pub outbox_min_free_bytes: u64,
    /// Percentage free-space reserve kept on the filesystem containing the
    /// outbox. The effective reserve is the stricter of this value and
    /// `outbox_min_free_bytes`.
    #[serde(default = "default_outbox_min_free_percent")]
    pub outbox_min_free_percent: u8,
    #[serde(default)]
    pub exhausted_delivery_policy: ExhaustedDeliveryPolicy,
}

impl Default for EventDispatcherConfig {
    fn default() -> Self {
        Self {
            max_in_memory_deliveries: default_dispatcher_max_in_memory_deliveries(),
            replay_batch_size: default_dispatcher_replay_batch_size(),
            max_retry_attempts: default_dispatcher_max_retry_attempts(),
            retry_initial_delay_ms: default_dispatcher_retry_initial_delay_ms(),
            retry_max_delay_ms: default_dispatcher_retry_max_delay_ms(),
            webhook_timeout_ms: default_webhook_timeout_ms(),
            outbox_min_free_bytes: default_outbox_min_free_bytes(),
            outbox_min_free_percent: default_outbox_min_free_percent(),
            exhausted_delivery_policy: ExhaustedDeliveryPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExhaustedDeliveryPolicy {
    /// Keep retryable deliveries durable and continue retrying with bounded
    /// backoff after `max_retry_attempts`.
    #[default]
    RetryForever,
    /// Write the event to `dead_letter_path` and acknowledge the outbox entry.
    DeadLetter,
    /// Acknowledge the outbox entry after logging the exhausted delivery.
    Discard,
}

fn default_dispatcher_max_in_memory_deliveries() -> usize {
    4_096
}

fn default_dispatcher_replay_batch_size() -> usize {
    4_096
}

fn default_dispatcher_max_retry_attempts() -> u32 {
    10
}

fn default_dispatcher_retry_initial_delay_ms() -> u64 {
    4_000
}

fn default_dispatcher_retry_max_delay_ms() -> u64 {
    64_000
}

fn default_webhook_timeout_ms() -> u64 {
    10_000
}

fn default_outbox_min_free_bytes() -> u64 {
    1024 * 1024 * 1024
}

fn default_outbox_min_free_percent() -> u8 {
    5
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum EventSinkConfig {
    #[serde(rename = "jsonl")]
    JsonLines {
        tag: String,
        path: String,
        #[serde(default)]
        events: Vec<String>,
        #[serde(default)]
        source_id: Option<String>,
    },
    #[serde(rename = "webhook")]
    Webhook {
        /// Local registration identity used by delivery status and the outbox.
        tag: String,
        /// Complete receiver URL. Zero sends requests to this value unchanged.
        ///
        /// The same URL may be used by multiple registrations or Zero
        /// instances; the URL does not identify a node or proxy protocol.
        url: String,
        /// Event-type allowlist for this registration. Empty accepts all events.
        #[serde(default)]
        events: Vec<String>,
        /// Optional producer metadata copied into delivered event envelopes.
        ///
        /// This value does not participate in registration or routing.
        #[serde(default)]
        source_id: Option<String>,
        /// Opaque HTTP headers supplied by the sink registrant.
        #[serde(default)]
        headers: BTreeMap<String, String>,
        #[serde(default)]
        allow_insecure: bool,
    },
}

impl EventSinkConfig {
    pub fn tag(&self) -> &str {
        match self {
            Self::JsonLines { tag, .. } | Self::Webhook { tag, .. } => tag,
        }
    }

    pub fn events(&self) -> &[String] {
        match self {
            Self::JsonLines { events, .. } | Self::Webhook { events, .. } => events,
        }
    }

    pub fn source_id(&self) -> Option<&str> {
        match self {
            Self::JsonLines { source_id, .. } | Self::Webhook { source_id, .. } => {
                source_id.as_deref()
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ControlApiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub listen: Option<ListenConfig>,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub api_key_env: Option<String>,
    /// Optional gRPC transport and authentication policy. This does not
    /// change the HTTP control endpoint.
    #[serde(default)]
    pub grpc: Option<ControlGrpcConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlGrpcConfig {
    /// Allow plaintext gRPC on a non-loopback listener. Intended for trusted
    /// private networks or an external TLS terminator.
    #[serde(default)]
    pub allow_insecure_remote: bool,
    /// Reuse `api.control`'s bearer credential for gRPC authentication.
    #[serde(default = "default_true")]
    pub bearer_auth: bool,
    /// Native server TLS. Supplying `client_ca_cert_path` enables mTLS.
    #[serde(default)]
    pub tls: Option<ControlGrpcTlsConfig>,
}

impl Default for ControlGrpcConfig {
    fn default() -> Self {
        Self {
            allow_insecure_remote: false,
            bearer_auth: true,
            tls: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlGrpcTlsConfig {
    pub cert_path: String,
    pub key_path: String,
    #[serde(default)]
    pub client_ca_cert_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum HookConfig {
    #[serde(rename = "ipc")]
    Ipc {
        socket: String,
        #[serde(default = "default_hook_timeout_ms")]
        timeout_ms: u64,
    },
}

fn default_hook_timeout_ms() -> u64 {
    100
}

fn default_true() -> bool {
    true
}
