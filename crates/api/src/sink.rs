use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::{ApiError, ApiErrorCode, ApiResult, EventSink, PublishResult, RawApiEvent};
use serde::{Deserialize, Serialize};

pub struct CallbackEventSink<F> {
    name: String,
    callback: F,
}

impl<F> CallbackEventSink<F> {
    pub fn new(name: impl Into<String>, callback: F) -> Self {
        Self {
            name: name.into(),
            callback,
        }
    }
}

impl<F> EventSink for CallbackEventSink<F>
where
    F: Fn(&RawApiEvent) -> ApiResult<PublishResult> + Send + Sync,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult> {
        (self.callback)(event)
    }
}

#[derive(Debug)]
pub struct JsonLineEventSink<W> {
    name: String,
    writer: Mutex<W>,
}

impl<W> JsonLineEventSink<W>
where
    W: Write,
{
    pub fn new(writer: W) -> Self {
        Self {
            name: "jsonl".to_owned(),
            writer: Mutex::new(writer),
        }
    }

    pub fn named(name: impl Into<String>, writer: W) -> Self {
        Self {
            name: name.into(),
            writer: Mutex::new(writer),
        }
    }

    pub fn into_inner(self) -> ApiResult<W> {
        self.writer.into_inner().map_err(|_| {
            ApiError::new(
                ApiErrorCode::Internal,
                "json-line event sink lock was poisoned",
            )
        })
    }
}

impl<W> EventSink for JsonLineEventSink<W>
where
    W: Write + Send,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult> {
        let mut writer = self.writer.lock().map_err(|_| {
            ApiError::new(
                ApiErrorCode::Internal,
                "json-line event sink lock was poisoned",
            )
        })?;

        let mut frame = serde_json::to_vec(event).map_err(serialization_error)?;
        frame.push(b'\n');
        writer.write_all(&frame).map_err(io_error)?;
        writer.flush().map_err(io_error)?;

        Ok(PublishResult::delivered())
    }
}

fn serialization_error(error: serde_json::Error) -> ApiError {
    ApiError {
        code: ApiErrorCode::Internal,
        message: "failed to serialize event as json line".to_owned(),
        field_path: None,
        cause: Some(error.to_string()),
        details: Vec::new(),
    }
}

fn io_error(error: std::io::Error) -> ApiError {
    ApiError {
        code: ApiErrorCode::Internal,
        message: "failed to write event sink output".to_owned(),
        field_path: None,
        cause: Some(error.to_string()),
        details: Vec::new(),
    }
}

// ── MemorySink ───────────────────────────────────────────────────────

/// An in-memory event sink that collects events into a `Vec`.
/// Useful for testing and debugging.
#[derive(Debug)]
pub struct MemorySink {
    name: String,
    buffer: Mutex<Vec<RawApiEvent>>,
}

impl MemorySink {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            buffer: Mutex::new(Vec::new()),
        }
    }

    /// Drain all collected events.
    pub fn drain(&self) -> Vec<RawApiEvent> {
        self.buffer
            .lock()
            .expect("memory sink lock poisoned")
            .drain(..)
            .collect()
    }

    /// Take a snapshot of collected events without draining.
    pub fn snapshot(&self) -> Vec<RawApiEvent> {
        self.buffer
            .lock()
            .expect("memory sink lock poisoned")
            .clone()
    }

    pub fn len(&self) -> usize {
        self.buffer.lock().expect("memory sink lock poisoned").len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer
            .lock()
            .expect("memory sink lock poisoned")
            .is_empty()
    }
}

impl EventSink for MemorySink {
    fn name(&self) -> &str {
        &self.name
    }

    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult> {
        self.buffer
            .lock()
            .expect("memory sink lock poisoned")
            .push(event.clone());
        Ok(PublishResult::delivered())
    }
}

// ── SinkManager ──────────────────────────────────────────────────────

/// Per-sink delivery status snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkStatus {
    pub name: String,
    /// Current durable/in-memory backlog waiting for delivery or ACK.
    #[serde(default)]
    pub pending: u64,
    /// Current dispatcher-owned delivery lifecycle. This object is additive
    /// to the V1 status contract and is absent on older cores.
    #[serde(default, skip_serializing_if = "SinkDeliveryStatus::is_idle")]
    pub delivery: SinkDeliveryStatus,
    pub total_delivered: u64,
    pub total_failed: u64,
    /// Number of detected event-sequence gaps. Unlike `last_error`, this
    /// counter remains visible after later successful deliveries.
    #[serde(default)]
    pub replay_gaps: u64,
    pub last_success_at_unix_ms: Option<u64>,
    pub last_failure_at_unix_ms: Option<u64>,
    pub last_error: Option<String>,
    /// Filesystem safety state for the durable outbox, when enabled.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_storage: Option<OutboxStorageStatus>,
    /// Persistent journal recovery state. This remains visible after later
    /// successful deliveries so operators can reconcile preserved facts.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outbox_recovery: Option<OutboxRecoveryStatus>,
}

/// Per-sink delivery lifecycle owned by the asynchronous Connector
/// dispatcher. The fields intentionally remain independent because a durable
/// backlog may coexist with one in-flight request and another scheduled
/// retry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SinkDeliveryStatus {
    /// The per-sink worker currently owns one delivery request.
    #[serde(default)]
    pub in_flight: bool,
    /// Deliveries that have already failed at least once and remain queued for
    /// another publish attempt.
    #[serde(default)]
    pub retry_pending: u64,
    /// Successfully published deliveries whose durable outbox ACK must be
    /// retried.
    #[serde(default)]
    pub ack_retry_pending: u64,
    /// Outstanding entries currently owned by the durable outbox.
    #[serde(default)]
    pub durable_pending: u64,
    /// Earliest publish or ACK retry deadline, as Unix milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_retry_at_unix_ms: Option<u64>,
}

impl SinkDeliveryStatus {
    fn is_idle(&self) -> bool {
        *self == Self::default()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxRecoveryStatus {
    /// `recovered` for an automatically repaired partial tail, or
    /// `recovery_required` when a structurally corrupt generation was
    /// quarantined and delivery continued with a new journal.
    pub state: OutboxRecoveryState,
    pub corruption_class: OutboxCorruptionClass,
    /// Path containing the bytes rejected from the active journal.
    pub preserved_path: String,
    /// Bounded parse/recovery diagnostic; event payloads are not included.
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxRecoveryState {
    Recovered,
    RecoveryRequired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxCorruptionClass {
    PartialTail,
    MalformedTail,
    MalformedMiddle,
    MalformedJournal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutboxStorageStatus {
    pub available_bytes: u64,
    pub total_bytes: u64,
    pub reserve_bytes: u64,
    /// Emergency floor retained while writing ACKs or compacting the journal.
    pub maintenance_reserve_bytes: u64,
    /// True when new outbox PUT records are paused to preserve the reserve.
    pub write_blocked: bool,
}

/// Coordinates multiple `EventSink` instances with delivery tracking.
///
/// Events are delivered to all configured sinks in parallel. A sink that
/// returns a non-retryable failure is skipped; retryable failures are
/// returned to the caller for retry logic.
pub struct SinkManager {
    sinks: Vec<ManagedSink>,
    stats: Vec<SinkStats>,
}

struct SinkStats {
    delivered: AtomicU64,
    failed: AtomicU64,
    last_success: Mutex<Option<u64>>,
    last_failure: Mutex<Option<u64>>,
    last_error: Mutex<Option<String>>,
}

impl SinkStats {
    fn new() -> Self {
        Self {
            delivered: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            last_success: Mutex::new(None),
            last_failure: Mutex::new(None),
            last_error: Mutex::new(None),
        }
    }
}

impl Default for SinkManager {
    fn default() -> Self {
        Self::new()
    }
}

struct ManagedSink {
    sink: Box<dyn EventSink + Send + Sync>,
    filter: Option<Vec<String>>,
}

impl SinkManager {
    pub fn new() -> Self {
        Self {
            sinks: Vec::new(),
            stats: Vec::new(),
        }
    }

    /// Register a sink that accepts all event types.
    pub fn register(&mut self, sink: Box<dyn EventSink + Send + Sync>) {
        self.sinks.push(ManagedSink { sink, filter: None });
        self.stats.push(SinkStats::new());
    }

    /// Register a sink that only receives events whose `event_type` is in
    /// the provided allow-list.
    pub fn register_filtered(
        &mut self,
        sink: Box<dyn EventSink + Send + Sync>,
        event_types: Vec<String>,
    ) {
        self.sinks.push(ManagedSink {
            sink,
            filter: Some(event_types),
        });
        self.stats.push(SinkStats::new());
    }

    /// Snapshot delivery status for all sinks.
    pub fn status(&self) -> Vec<SinkStatus> {
        let _now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.sinks
            .iter()
            .zip(self.stats.iter())
            .map(|(managed, stat)| SinkStatus {
                name: managed.sink.name().to_owned(),
                pending: 0,
                delivery: SinkDeliveryStatus::default(),
                total_delivered: stat.delivered.load(Ordering::Relaxed),
                total_failed: stat.failed.load(Ordering::Relaxed),
                replay_gaps: 0,
                last_success_at_unix_ms: *stat.last_success.lock().unwrap(),
                last_failure_at_unix_ms: *stat.last_failure.lock().unwrap(),
                last_error: stat.last_error.lock().unwrap().clone(),
                outbox_storage: None,
                outbox_recovery: None,
            })
            .collect()
    }

    /// Publish an event to all configured sinks.
    ///
    /// Returns per-sink results.  Non-retryable failures are logged and
    /// dropped; retryable failures are returned so the caller can retry.
    pub fn publish(&self, event: &RawApiEvent) -> Vec<(String, PublishResult)> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.sinks
            .iter()
            .zip(self.stats.iter())
            .filter(|(managed, _)| accepts(managed, event))
            .map(|(managed, stat)| {
                let result = managed.sink.publish(event);
                match &result {
                    Ok(r) if r.delivered => {
                        stat.delivered.fetch_add(1, Ordering::Relaxed);
                        *stat.last_success.lock().unwrap() = Some(now_ms);
                        *stat.last_error.lock().unwrap() = None;
                    }
                    Ok(r) => {
                        stat.failed.fetch_add(1, Ordering::Relaxed);
                        *stat.last_failure.lock().unwrap() = Some(now_ms);
                        *stat.last_error.lock().unwrap() = r.message.clone();
                    }
                    Err(e) => {
                        stat.failed.fetch_add(1, Ordering::Relaxed);
                        *stat.last_failure.lock().unwrap() = Some(now_ms);
                        *stat.last_error.lock().unwrap() = Some(e.to_string());
                    }
                }
                match result {
                    Ok(r) => (managed.sink.name().to_owned(), r),
                    Err(e) => (
                        managed.sink.name().to_owned(),
                        PublishResult {
                            delivered: false,
                            retryable: false,
                            message: Some(e.to_string()),
                        },
                    ),
                }
            })
            .collect()
    }

    /// Flush all sinks.  Errors are silently ignored (individual sink
    /// implementations handle their own error logging).
    pub fn flush_all(&self) {
        for managed in &self.sinks {
            let _ = managed.sink.flush();
        }
    }

    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }
}

// ── RotatingFileSink ─────────────────────────────────────────────────

/// A JSON-line event sink that rotates files by size.
///
/// When the current file exceeds `max_bytes`, it is renamed to
/// `<path>.1`, `<path>.1` to `<path>.2`, and so on up to `max_files`.
/// A new file is then created for subsequent writes.
pub struct RotatingFileSink {
    name: String,
    path: std::path::PathBuf,
    max_bytes: u64,
    max_files: usize,
    writer: Mutex<RotatingWriter>,
}

struct RotatingWriter {
    file: std::fs::File,
    bytes_written: u64,
}

impl RotatingFileSink {
    pub fn new(
        name: impl Into<String>,
        path: impl Into<std::path::PathBuf>,
        max_bytes: u64,
        max_files: usize,
    ) -> ApiResult<Self> {
        let path = path.into();
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                ApiError::new(
                    ApiErrorCode::Internal,
                    format!("failed to open rotating sink file: {e}"),
                )
            })?;
        let metadata = file.metadata().map_err(|e| {
            ApiError::new(
                ApiErrorCode::Internal,
                format!("failed to stat sink file: {e}"),
            )
        })?;

        Ok(Self {
            name: name.into(),
            path,
            max_bytes,
            max_files,
            writer: Mutex::new(RotatingWriter {
                file,
                bytes_written: metadata.len(),
            }),
        })
    }

    fn rotate(&self, writer: &mut RotatingWriter) -> ApiResult<()> {
        // Rename existing rotated files: .2 -> .3, .1 -> .2, current -> .1
        for i in (1..self.max_files).rev() {
            let src = if i == 1 {
                self.path.clone()
            } else {
                self.path.with_extension(format!("{}", i - 1))
            };
            let dst = self.path.with_extension(format!("{i}"));
            if src.exists() {
                let _ = std::fs::rename(&src, &dst);
            }
        }

        // Open a fresh file.
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| {
                ApiError::new(
                    ApiErrorCode::Internal,
                    format!("failed to reopen rotating sink: {e}"),
                )
            })?;

        writer.file = file;
        writer.bytes_written = 0;
        Ok(())
    }
}

impl EventSink for RotatingFileSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult> {
        let mut writer = self
            .writer
            .lock()
            .map_err(|_| ApiError::new(ApiErrorCode::Internal, "rotating sink lock poisoned"))?;

        let line = serde_json::to_vec(event).map_err(|e| {
            ApiError::new(
                ApiErrorCode::Internal,
                format!("failed to serialize event: {e}"),
            )
        })?;
        let frame = [line.as_slice(), b"\n"].concat();

        if writer.bytes_written + frame.len() as u64 > self.max_bytes {
            self.rotate(&mut writer)?;
        }

        use std::io::Write;
        writer.file.write_all(&frame).map_err(|e| {
            ApiError::new(
                ApiErrorCode::Internal,
                format!("failed to write rotating sink: {e}"),
            )
        })?;
        writer.file.flush().map_err(|e| {
            ApiError::new(
                ApiErrorCode::Internal,
                format!("failed to flush rotating sink: {e}"),
            )
        })?;
        writer.bytes_written += frame.len() as u64;

        Ok(PublishResult::delivered())
    }
}

fn accepts(managed: &ManagedSink, event: &RawApiEvent) -> bool {
    match &managed.filter {
        None => true,
        Some(types) => types.is_empty() || types.iter().any(|t| t == &event.event_type),
    }
}

// ── DeadLetterSink ────────────────────────────────────────────────────

/// A persistent sink that writes failed deliveries to a JSON-line file.
///
/// When events exhaust retry attempts in the event dispatcher, they are
/// written here so they are never silently lost.  The file can be replayed
/// or inspected offline.
pub struct DeadLetterSink {
    name: String,
    path: std::path::PathBuf,
    writer: Mutex<std::fs::File>,
}

impl DeadLetterSink {
    pub fn new(path: impl Into<std::path::PathBuf>) -> ApiResult<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| {
                ApiError::new(
                    ApiErrorCode::Internal,
                    format!("failed to create dead-letter directory: {e}"),
                )
            })?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                ApiError::new(
                    ApiErrorCode::Internal,
                    format!("failed to open dead-letter file `{}`: {e}", path.display()),
                )
            })?;
        Ok(Self {
            name: "dead-letter".to_owned(),
            path,
            writer: Mutex::new(file),
        })
    }

    /// Number of bytes written to the dead-letter file.
    pub fn size_bytes(&self) -> ApiResult<u64> {
        self.writer
            .lock()
            .map_err(|_| ApiError::new(ApiErrorCode::Internal, "dead-letter lock poisoned"))
            .and_then(|f| {
                f.metadata().map(|m| m.len()).map_err(|e| {
                    ApiError::new(
                        ApiErrorCode::Internal,
                        format!("failed to stat dead-letter file: {e}"),
                    )
                })
            })
    }
}

impl std::fmt::Debug for DeadLetterSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeadLetterSink")
            .field("name", &self.name)
            .field("path", &self.path)
            .finish()
    }
}

impl EventSink for DeadLetterSink {
    fn name(&self) -> &str {
        &self.name
    }

    fn publish(&self, event: &RawApiEvent) -> ApiResult<PublishResult> {
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let envelope = serde_json::json!({
            "dead_lettered_at_unix_ms": now_ms,
            "original_event": event,
        });

        let mut line = serde_json::to_vec(&envelope).map_err(|e| {
            ApiError::new(
                ApiErrorCode::Internal,
                format!("dead-letter serialization: {e}"),
            )
        })?;
        line.push(b'\n');

        let mut f = self
            .writer
            .lock()
            .map_err(|_| ApiError::new(ApiErrorCode::Internal, "dead-letter lock poisoned"))?;
        f.write_all(&line).map_err(|e| {
            ApiError::new(ApiErrorCode::Internal, format!("dead-letter write: {e}"))
        })?;
        f.flush().ok();

        Ok(PublishResult::delivered())
    }
}
