use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tracing::warn;
use zero_api::{OutboxRecoveryStatus, OutboxStorageStatus, SinkDeliveryStatus, SinkStatus};

use super::*;

/// Per-sink delivery counters, updated by the dispatcher thread.
pub(in crate::dispatcher) struct PerSinkStats {
    pub(in crate::dispatcher) source_buffer: Mutex<Option<Arc<source::SourceBudget>>>,
    pub(in crate::dispatcher) delivery_buffer: Mutex<Option<Arc<memory::DeliveryBudget>>>,
    pending: AtomicU64,
    total_delivered: AtomicU64,
    total_failed: AtomicU64,
    replay_gaps: AtomicU64,
    delivery: Mutex<SinkDeliveryStatus>,
    last_success_ms: Mutex<Option<u64>>,
    last_failure_ms: Mutex<Option<u64>>,
    last_error: Mutex<Option<String>>,
    outbox_storage: Mutex<Option<OutboxStorageStatus>>,
    outbox_recovery: Mutex<Option<OutboxRecoveryStatus>>,
}

impl PerSinkStats {
    pub(in crate::dispatcher) fn new() -> Self {
        Self {
            source_buffer: Mutex::new(None),
            delivery_buffer: Mutex::new(None),
            pending: AtomicU64::new(0),
            total_delivered: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            replay_gaps: AtomicU64::new(0),
            delivery: Mutex::new(SinkDeliveryStatus::default()),
            last_success_ms: Mutex::new(None),
            last_failure_ms: Mutex::new(None),
            last_error: Mutex::new(None),
            outbox_storage: Mutex::new(None),
            outbox_recovery: Mutex::new(None),
        }
    }

    fn set_pending(&self, pending: usize) {
        self.pending.store(pending as u64, Ordering::Relaxed);
    }

    pub(in crate::dispatcher) fn record_delivered(&self) {
        self.total_delivered.fetch_add(1, Ordering::Relaxed);
        *self.last_success_ms.lock().expect("sink stats") = Some(now_unix_ms());
        *self.last_error.lock().expect("sink stats") = None;
    }

    pub(in crate::dispatcher) fn record_failed(&self, message: Option<String>) {
        self.total_failed.fetch_add(1, Ordering::Relaxed);
        *self.last_failure_ms.lock().expect("sink stats") = Some(now_unix_ms());
        *self.last_error.lock().expect("sink stats") = message;
    }

    pub(in crate::dispatcher) fn snapshot(&self, name: String) -> SinkStatus {
        SinkStatus {
            delivery_buffer: self
                .delivery_buffer
                .lock()
                .expect("sink stats")
                .as_ref()
                .map(|budget| budget.snapshot()),
            source_buffer: self
                .source_buffer
                .lock()
                .expect("sink stats")
                .as_ref()
                .map(|budget| budget.snapshot()),
            name,
            pending: self.pending.load(Ordering::Relaxed),
            delivery: *self.delivery.lock().expect("sink stats"),
            total_delivered: self.total_delivered.load(Ordering::Relaxed),
            total_failed: self.total_failed.load(Ordering::Relaxed),
            replay_gaps: self.replay_gaps.load(Ordering::Relaxed),
            last_success_at_unix_ms: *self.last_success_ms.lock().expect("sink stats"),
            last_failure_at_unix_ms: *self.last_failure_ms.lock().expect("sink stats"),
            last_error: self.last_error.lock().expect("sink stats").clone(),
            outbox_storage: *self.outbox_storage.lock().expect("sink stats"),
            outbox_recovery: self.outbox_recovery.lock().expect("sink stats").clone(),
        }
    }
}

pub(in crate::dispatcher) fn refresh_pending_stats(
    stats: &SharedSinkStats,
    pending: &VecDeque<PendingDelivery>,
    workers: &[SinkWorker],
    outbox: Option<&DeliveryOutbox>,
) {
    let now = Instant::now();
    let now_unix_ms = now_unix_ms();
    let storage_status = outbox.map(DeliveryOutbox::storage_status);
    let recovery_status = outbox.and_then(DeliveryOutbox::recovery_status);
    let mut pending_counts = BTreeMap::<String, usize>::new();
    if let Some(outbox) = outbox {
        let tags = stats
            .lock()
            .expect("sink stats")
            .iter()
            .map(|(tag, _)| tag.clone())
            .collect::<Vec<_>>();
        match outbox.pending_counts_for(tags.iter().map(String::as_str)) {
            Ok(counts) => pending_counts = counts,
            Err(error) => {
                for (_, stat) in stats.lock().expect("sink stats").iter() {
                    stat.record_failed(Some(error.to_string()));
                }
                return;
            }
        }
    } else {
        for delivery in pending {
            *pending_counts.entry(delivery.sink_tag.clone()).or_default() += 1;
        }
        for worker in workers {
            if worker.in_flight.is_some() {
                *pending_counts.entry(worker.tag.clone()).or_default() += 1;
            }
        }
    }
    let shared = stats.lock().expect("sink stats");
    for (tag, stat) in shared.iter() {
        stat.set_pending(*pending_counts.get(tag).unwrap_or(&0));
        *stat.delivery.lock().expect("sink stats") = SinkDeliveryStatus::default();
        *stat.outbox_recovery.lock().expect("sink stats") = recovery_status.clone();
        *stat.outbox_storage.lock().expect("sink stats") = match &storage_status {
            Some(Ok(status)) => Some(*status),
            _ => None,
        };
        if let Some(Err(error)) = &storage_status {
            *stat.last_error.lock().expect("sink stats") =
                Some(format!("failed to inspect outbox storage: {error}"));
        }
    }
    if outbox.is_some() {
        for (sink_tag, count) in &pending_counts {
            if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == sink_tag) {
                stat.delivery.lock().expect("sink stats").durable_pending = *count as u64;
            }
        }
    }
    for delivery in pending {
        if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == &delivery.sink_tag) {
            let mut status = stat.delivery.lock().expect("sink stats");
            if delivery.awaiting_ack {
                status.ack_retry_pending = status.ack_retry_pending.saturating_add(1);
                record_retry_deadline(&mut status, now, now_unix_ms, delivery.next_due);
            } else if delivery.attempts > 0 {
                status.retry_pending = status.retry_pending.saturating_add(1);
                record_retry_deadline(&mut status, now, now_unix_ms, delivery.next_due);
            }
        }
    }
    for worker in workers {
        if worker.in_flight.is_some() {
            if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == &worker.tag) {
                stat.delivery.lock().expect("sink stats").in_flight = true;
            }
        }
    }
}

fn record_retry_deadline(
    status: &mut SinkDeliveryStatus,
    now: Instant,
    now_unix_ms: u64,
    due: Instant,
) {
    let delay_ms =
        u64::try_from(due.saturating_duration_since(now).as_millis()).unwrap_or(u64::MAX);
    let deadline = now_unix_ms.saturating_add(delay_ms);
    status.next_retry_at_unix_ms = Some(
        status
            .next_retry_at_unix_ms
            .map_or(deadline, |current| current.min(deadline)),
    );
}

pub(in crate::dispatcher) fn record_delivery(
    stats: &SharedSinkStats,
    sink_tag: &str,
    result: &Result<zero_api::PublishResult, zero_api::ApiError>,
) {
    let shared = stats.lock().expect("sink stats");
    let Some(entry) = shared.iter().find(|(tag, _)| tag == sink_tag) else {
        return;
    };
    let s = &entry.1;
    match result {
        Ok(r) if r.delivered => s.record_delivered(),
        Ok(r) => s.record_failed(r.message.clone()),
        Err(e) => s.record_failed(Some(e.to_string())),
    }
}

pub(in crate::dispatcher) fn record_outbox_failure(
    stats: &SharedSinkStats,
    sink_tag: &str,
    error: &ConnectorError,
) {
    let shared = stats.lock().expect("sink stats");
    if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == sink_tag) {
        stat.record_failed(Some(error.to_string()));
    }
}

pub(in crate::dispatcher) fn record_outbox_ack_failure(
    stats: &SharedSinkStats,
    sink_tag: &str,
    error: &ConnectorError,
) {
    warn!(
        sink = sink_tag,
        error = %error,
        "remote delivery completed but local outbox ACK failed; retaining ACK-only backlog"
    );
    let shared = stats.lock().expect("sink stats");
    if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == sink_tag) {
        stat.record_failed(Some(format!("local outbox ACK failed: {error}")));
    }
}

pub(in crate::dispatcher) fn record_outbox_ack_recovered(stats: &SharedSinkStats, sink_tag: &str) {
    let shared = stats.lock().expect("sink stats");
    if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == sink_tag) {
        *stat.last_success_ms.lock().expect("sink stats") = Some(now_unix_ms());
        *stat.last_error.lock().expect("sink stats") = None;
    }
}

pub(in crate::dispatcher) fn record_dispatcher_failure(stats: &SharedSinkStats, message: &str) {
    let shared = stats.lock().expect("sink stats");
    for (_, stat) in shared.iter() {
        stat.record_failed(Some(message.to_owned()));
    }
}

pub(in crate::dispatcher) fn record_dispatcher_replay_gap(stats: &SharedSinkStats, message: &str) {
    let shared = stats.lock().expect("sink stats");
    for (_, stat) in shared.iter() {
        stat.replay_gaps.fetch_add(1, Ordering::Relaxed);
        stat.record_failed(Some(message.to_owned()));
    }
}

pub(in crate::dispatcher) fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
