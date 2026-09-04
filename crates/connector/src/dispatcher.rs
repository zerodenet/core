use std::collections::{BTreeMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::task::JoinHandle;
use tracing::{debug, warn};
use zero_api::{
    event_type, DeadLetterSink, EventFilter, EventSink, EventSource, EventStream,
    EventStreamReceive, OutboxRecoveryStatus, OutboxStorageStatus, RawApiEvent, SinkDeliveryStatus,
    SinkStatus,
};
use zero_config::{ApiConfig, EventDispatcherConfig};

use crate::network::EventDispatcherNetwork;
use crate::registry::{build_event_sinks, resolve_path, ConfiguredEventSink};
use crate::{ConnectorError, ConnectorResult};

pub(crate) mod outbox;
mod worker;

use outbox::{DeliveryOutbox, OutboxDelivery};
use worker::{SinkPublishResult, SinkWorker, SinkWorkerResult};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(1);

type SharedSinkStats = Arc<Mutex<Vec<(String, Arc<PerSinkStats>)>>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventDispatcherOptions {
    pub poll_interval: Duration,
    /// Test/embedding override. Zero uses `api.dispatcher.max_retry_attempts`
    /// when this value is zero.
    pub max_retry_attempts: u32,
}

impl Default for EventDispatcherOptions {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
            max_retry_attempts: 0,
        }
    }
}

/// Per-sink delivery counters, updated by the dispatcher thread.
struct PerSinkStats {
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
    fn new() -> Self {
        Self {
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

    fn record_delivered(&self) {
        self.total_delivered.fetch_add(1, Ordering::Relaxed);
        *self.last_success_ms.lock().expect("sink stats") = Some(now_unix_ms());
        *self.last_error.lock().expect("sink stats") = None;
    }

    fn record_failed(&self, message: Option<String>) {
        self.total_failed.fetch_add(1, Ordering::Relaxed);
        *self.last_failure_ms.lock().expect("sink stats") = Some(now_unix_ms());
        *self.last_error.lock().expect("sink stats") = message;
    }

    fn snapshot(&self, name: String) -> SinkStatus {
        SinkStatus {
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

pub struct EventDispatcherHandle {
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: JoinHandle<()>,
    /// Per-sink delivery stats shared with the dispatcher thread.
    sink_stats: Arc<Vec<(String, Arc<PerSinkStats>)>>,
}

#[derive(Clone)]
pub struct EventDispatcherStatusHandle {
    sink_stats: Arc<Vec<(String, Arc<PerSinkStats>)>>,
}

impl EventDispatcherHandle {
    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        let _ = self.task.await;
    }

    /// Snapshot the current delivery status for all configured sinks.
    pub fn sink_status(&self) -> Vec<SinkStatus> {
        self.status_handle().sink_status()
    }

    pub fn status_handle(&self) -> EventDispatcherStatusHandle {
        EventDispatcherStatusHandle {
            sink_stats: self.sink_stats.clone(),
        }
    }
}

impl EventDispatcherStatusHandle {
    /// Snapshot the current delivery status for all configured sinks.
    pub fn sink_status(&self) -> Vec<SinkStatus> {
        self.sink_stats
            .iter()
            .map(|(tag, stats)| stats.snapshot(tag.clone()))
            .collect()
    }
}

pub fn spawn_event_dispatcher<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(
        source,
        api,
        source_dir,
        options,
        EventDispatcherNetwork::system(),
        false,
    )
}

/// Start an event dispatcher with an explicitly supplied network boundary.
///
/// The Zero application uses this entrypoint to share the same physical-egress
/// authority as proxy and DNS sockets. Standalone embeddings that cannot run a
/// TUN data plane may continue to use [`spawn_event_dispatcher`].
pub fn spawn_event_dispatcher_with_network<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(source, api, source_dir, options, network, false)
}

/// Start an event dispatcher and bootstrap the retained `engine.started`
/// lifecycle fact after the live subscription has been registered.
///
/// The application uses this once for the first configured delivery sink in
/// an engine process. Registering the live subscription first closes the race
/// with a concurrent startup event; sequence de-duplication then guarantees
/// that an event observed through both paths is delivered only once.
pub fn spawn_event_dispatcher_with_engine_started<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(
        source,
        api,
        source_dir,
        options,
        EventDispatcherNetwork::system(),
        true,
    )
}

/// Start an explicitly network-bound event dispatcher and bootstrap the
/// retained `engine.started` fact.
pub fn spawn_event_dispatcher_with_engine_started_and_network<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    spawn_event_dispatcher_inner(source, api, source_dir, options, network, true)
}

fn spawn_event_dispatcher_inner<S>(
    source: S,
    api: ApiConfig,
    source_dir: Option<PathBuf>,
    options: EventDispatcherOptions,
    network: EventDispatcherNetwork,
    bootstrap_engine_started: bool,
) -> ConnectorResult<Option<EventDispatcherHandle>>
where
    S: EventSource + Send + Sync + 'static,
{
    let (init_tx, init_rx) = mpsc::sync_channel(1);
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();

    let dead_letter_path = api.dead_letter_path.clone();
    let outbox_path = api.outbox_path.clone();

    // Shared stats: populated by the dispatcher thread after sink construction,
    // read by the handle on demand.  Created empty here; tags are filled in
    // before the init signal so the handle always sees valid data.
    let sink_stats: SharedSinkStats = Arc::new(Mutex::new(Vec::new()));
    let stats_for_handle = sink_stats.clone();
    let stats_for_thread = sink_stats.clone();

    let task = tokio::task::spawn_blocking(move || {
        let sinks = match build_event_sinks(&api, source_dir.as_deref(), &network) {
            Ok(sinks) => sinks,
            Err(error) => {
                let _ = init_tx.send(Err(error));
                return;
            }
        };

        if sinks.is_empty() && dead_letter_path.is_none() {
            let _ = init_tx.send(Ok(false));
            return;
        }

        // Initialise per-sink stats with the constructed tags.
        {
            let mut shared = stats_for_thread.lock().expect("sink stats");
            for sink in &sinks {
                shared.push((sink.tag.clone(), Arc::new(PerSinkStats::new())));
            }
        }

        let (dead_letter, _dead_letter_lease) = match dead_letter_path {
            Some(path) => {
                let path = resolve_path(&path, source_dir.as_deref());
                let lease = match crate::state::PersistentStateLease::acquire(&path) {
                    Ok(lease) => lease,
                    Err(error) => {
                        let _ = init_tx.send(Err(error));
                        return;
                    }
                };
                let sink = match DeadLetterSink::new(&path) {
                    Ok(sink) => sink,
                    Err(error) => {
                        let _ = init_tx.send(Err(error.into()));
                        return;
                    }
                };
                debug!(path = %path.display(), "dead-letter sink enabled");
                (Some(sink), Some(lease))
            }
            None => (None, None),
        };

        let dispatcher_config = api.dispatcher;
        let mut outbox = match outbox_path {
            Some(path) => match DeliveryOutbox::open(
                &path,
                source_dir.as_deref(),
                dispatcher_config.outbox_min_free_bytes,
                dispatcher_config.outbox_min_free_percent,
            ) {
                Ok(outbox) => Some(outbox),
                Err(error) => {
                    let _ = init_tx.send(Err(error));
                    return;
                }
            },
            None => None,
        };

        let recovered = match outbox.as_ref() {
            Some(outbox) => match outbox
                .load_pending_excluding(&HashSet::new(), dispatcher_config.max_in_memory_deliveries)
            {
                Ok(recovered) => recovered,
                Err(error) => {
                    let _ = init_tx.send(Err(error));
                    return;
                }
            },
            None => Vec::new(),
        };

        let subscriber = match source.subscribe(EventFilter::default()) {
            Ok(subscriber) => subscriber,
            Err(error) => {
                let _ = init_tx.send(Err(ConnectorError::Config(format!(
                    "event dispatcher failed to subscribe to source: {error}"
                ))));
                return;
            }
        };
        let bootstrap_events = if bootstrap_engine_started {
            match source.latest(
                1,
                EventFilter {
                    event_types: vec![event_type::ENGINE_STARTED.to_owned()],
                    ..EventFilter::default()
                },
            ) {
                Ok(events) => events,
                Err(error) => {
                    let _ = init_tx.send(Err(ConnectorError::Api(error)));
                    return;
                }
            }
        } else {
            Vec::new()
        };
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = init_tx.send(Err(ConnectorError::Config(format!(
                    "failed to build connector dispatcher runtime: {error}"
                ))));
                return;
            }
        };
        let _ = init_tx.send(Ok(true));
        runtime.block_on(run_event_dispatcher(
            source,
            subscriber,
            bootstrap_events,
            sinks,
            &stats_for_thread,
            options,
            shutdown_rx,
            dead_letter,
            &mut outbox,
            recovered,
            dispatcher_config,
        ));
    });

    let init_result = init_rx
        .recv()
        .map_err(|_| ConnectorError::DispatcherStart)??;

    if !init_result {
        return Ok(None);
    }

    // Stats are now populated; take them out of the mutex for the handle.
    let stats_snapshot = stats_for_handle.lock().expect("sink stats").clone();

    Ok(Some(EventDispatcherHandle {
        shutdown: Some(shutdown_tx),
        task,
        sink_stats: Arc::new(stats_snapshot),
    }))
}

#[allow(clippy::too_many_arguments)]
async fn run_event_dispatcher<S>(
    source: S,
    subscriber: S::Stream,
    bootstrap_events: Vec<RawApiEvent>,
    sinks: Vec<ConfiguredEventSink>,
    stats: &SharedSinkStats,
    options: EventDispatcherOptions,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    dead_letter: Option<DeadLetterSink>,
    outbox: &mut Option<DeliveryOutbox>,
    recovered: Vec<OutboxDelivery>,
    dispatcher_config: EventDispatcherConfig,
) where
    S: EventSource + Send + Sync + 'static,
{
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut workers = sinks
        .into_iter()
        .map(|sink| SinkWorker::spawn(sink, result_tx.clone()))
        .collect::<Vec<_>>();
    drop(result_tx);
    let source_cancelled = Arc::new(AtomicBool::new(false));
    let (source_tx, mut source_rx) =
        tokio::sync::mpsc::channel(dispatcher_config.replay_batch_size.max(1));
    let source_cancelled_for_thread = source_cancelled.clone();
    let source_task = std::thread::Builder::new()
        .name("zero-event-source".to_owned())
        .spawn(move || {
            while !source_cancelled_for_thread.load(Ordering::Acquire) {
                match subscriber.recv_timeout(Duration::from_millis(250)) {
                    EventStreamReceive::Event(event) => {
                        if source_tx.blocking_send(*event).is_err() {
                            break;
                        }
                    }
                    EventStreamReceive::Timeout => {}
                    EventStreamReceive::Closed => break,
                }
            }
            while let Some(event) = subscriber.try_recv() {
                if source_tx.blocking_send(event).is_err() {
                    break;
                }
            }
        })
        .expect("event source bridge thread");
    let mut pending = recovered
        .into_iter()
        .map(PendingDelivery::recovered)
        .collect::<VecDeque<_>>();
    refresh_pending_stats(stats, &pending, &workers, outbox.as_ref());
    let mut last_sequence = None;
    let mut blocked_events = bootstrap_events
        .into_iter()
        .map(BlockedEvent::new)
        .collect::<VecDeque<_>>();
    let mut source_events = VecDeque::new();
    let mut completed_results = VecDeque::new();
    let mut source_closed = false;
    let max_retry_attempts = if options.max_retry_attempts == 0 {
        dispatcher_config.max_retry_attempts
    } else {
        options.max_retry_attempts
    };

    let mut shutting_down = false;
    loop {
        drain_worker_results(
            &mut workers,
            stats,
            &mut pending,
            max_retry_attempts,
            dead_letter.as_ref(),
            outbox,
            dispatcher_config,
            &mut result_rx,
            &mut completed_results,
        );
        if !shutting_down {
            fill_pending_from_outbox(
                stats,
                &mut pending,
                &workers,
                outbox.as_ref(),
                dispatcher_config.max_in_memory_deliveries,
            );
        }
        drain_event_source(
            &source,
            &mut source_rx,
            &mut source_events,
            &workers,
            stats,
            &mut pending,
            outbox,
            &mut last_sequence,
            &mut blocked_events,
            dispatcher_config,
        );
        submit_pending(
            &mut workers,
            stats,
            &mut pending,
            dead_letter.as_ref(),
            outbox,
            dispatcher_config,
            !shutting_down,
        );
        refresh_pending_stats(stats, &pending, &workers, outbox.as_ref());

        if shutting_down {
            let has_in_flight = workers.iter().any(|worker| worker.in_flight.is_some());
            let has_ready = pending
                .iter()
                .any(|delivery| !delivery.uses_outbox && delivery.next_due <= Instant::now());
            if source_closed && !has_in_flight && !has_ready {
                break;
            }
        }

        let wait_interval = next_wait_interval(
            &pending,
            &workers,
            &blocked_events,
            options.poll_interval,
            !shutting_down,
        );
        tokio::select! {
            biased;
            _ = &mut shutdown, if !shutting_down => {
                shutting_down = true;
                source_cancelled.store(true, Ordering::Release);
                for worker in &mut workers {
                    worker.cancel_durable_in_flight();
                }
                while drain_event_source(
                    &source,
                    &mut source_rx,
                    &mut source_events,
                    &workers,
                    stats,
                    &mut pending,
                    outbox,
                    &mut last_sequence,
                    &mut blocked_events,
                    dispatcher_config,
                ) {}
                if let Some(blocked) = blocked_events.front() {
                    warn!(
                        event_id = %blocked.event.event_id,
                        deferred_events = blocked_events.len(),
                        "event dispatcher stopped with events that could not be persisted; the event cursor was not advanced"
                    );
                }
            }
            Some(result) = result_rx.recv() => completed_results.push_back(result),
            event = source_rx.recv(), if !source_closed => match event {
                Some(event) => source_events.push_back(event),
                None => source_closed = true,
            },
            _ = tokio::time::sleep(wait_interval) => {}
        }
    }

    source_cancelled.store(true, Ordering::Release);
    for worker in &mut workers {
        worker.stop();
    }
    for worker in &mut workers {
        worker.join().await;
    }
    let _ = source_task.join();
    drain_worker_results(
        &mut workers,
        stats,
        &mut pending,
        max_retry_attempts,
        dead_letter.as_ref(),
        outbox,
        dispatcher_config,
        &mut result_rx,
        &mut completed_results,
    );
    refresh_pending_stats(stats, &pending, &workers, outbox.as_ref());

    debug!("event dispatcher stopped");
}

#[allow(clippy::too_many_arguments)]
fn drain_event_source<S>(
    source: &S,
    source_rx: &mut tokio::sync::mpsc::Receiver<RawApiEvent>,
    source_events: &mut VecDeque<RawApiEvent>,
    workers: &[SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    outbox: &mut Option<DeliveryOutbox>,
    last_sequence: &mut Option<u64>,
    blocked_events: &mut VecDeque<BlockedEvent>,
    dispatcher_config: EventDispatcherConfig,
) -> bool
where
    S: EventSource,
{
    let mut progressed = false;
    while let Some(blocked) = blocked_events.front_mut() {
        if blocked
            .persistence_retry_due
            .is_some_and(|due| due > Instant::now())
        {
            return false;
        }
        if let Some(sequence) = blocked.event.sequence {
            if last_sequence.is_some_and(|last| sequence <= last) {
                blocked_events.pop_front();
                continue;
            }
        }
        if blocked.event.event_type != event_type::FLOW_SNAPSHOT
            && !dispatch_event(workers, stats, pending, blocked, outbox, dispatcher_config)
        {
            return false;
        }
        if let Some(sequence) = blocked.event.sequence {
            *last_sequence = Some(sequence);
        }
        blocked_events.pop_front();
        progressed = true;
    }

    let mut events = Vec::new();
    while events.len() < dispatcher_config.replay_batch_size {
        let Some(event) = source_events.pop_front() else {
            break;
        };
        events.push(event);
    }
    while events.len() < dispatcher_config.replay_batch_size {
        match source_rx.try_recv() {
            Ok(event) => events.push(event),
            Err(_) => break,
        }
    }

    let replay_room = dispatcher_config
        .replay_batch_size
        .saturating_sub(events.len());
    if let Some(sequence) = (*last_sequence).filter(|_| replay_room > 0) {
        match source.since(sequence, replay_room, EventFilter::default()) {
            Ok(replay) => {
                if replay.has_gap {
                    let message = format!(
                        "event replay gap after sequence {}; retained events start at {}",
                        replay.requested_after, replay.actual_from
                    );
                    record_dispatcher_replay_gap(stats, &message);
                    warn!(
                        requested_after = replay.requested_after,
                        actual_from = replay.actual_from,
                        "event dispatcher replay gap detected; older sink deliveries require reconciliation"
                    );
                    *last_sequence = Some(replay.actual_from.saturating_sub(1));
                }
                events.extend(replay.events);
            }
            Err(error) => {
                record_dispatcher_failure(stats, &error.to_string());
                warn!(%error, "failed to replay dispatcher events");
            }
        }
    }

    if events.is_empty() {
        return progressed;
    }
    events.sort_by_key(|event| event.sequence.unwrap_or(u64::MAX));
    let mut events = events.into_iter();
    while let Some(event) = events.next() {
        if let Some(sequence) = event.sequence {
            if last_sequence.is_some_and(|last| sequence <= last) {
                continue;
            }
            if last_sequence.is_some_and(|last| sequence > last.saturating_add(1)) {
                // Keep the cursor before the missing range. The popped live
                // event remains in the engine log and will be replayed after
                // the missing range on the next pass.
                break;
            }
        }

        // `flow.snapshot` is a synchronization baseline for live clients.
        // Persistent sinks consume lifecycle facts only and must not bill or
        // audit the same active flow again whenever a dispatcher starts.
        if event.event_type != event_type::FLOW_SNAPSHOT {
            let mut blocked = BlockedEvent::new(event);
            if !dispatch_event(
                workers,
                stats,
                pending,
                &mut blocked,
                outbox,
                dispatcher_config,
            ) {
                blocked_events.push_back(blocked);
                blocked_events.extend(events.map(BlockedEvent::new));
                return progressed;
            }
            if let Some(sequence) = blocked.event.sequence {
                *last_sequence = Some(sequence);
            }
        } else if let Some(sequence) = event.sequence {
            *last_sequence = Some(sequence);
        }
        progressed = true;
    }
    progressed
}

fn refresh_pending_stats(
    stats: &SharedSinkStats,
    pending: &VecDeque<PendingDelivery>,
    workers: &[SinkWorker],
    outbox: Option<&DeliveryOutbox>,
) {
    let now = Instant::now();
    let now_unix_ms = now_unix_ms();
    let storage_status = outbox.map(DeliveryOutbox::storage_status);
    let recovery_status = outbox.and_then(DeliveryOutbox::recovery_status);
    // Build the complete count before publishing it. Resetting the atomic to
    // zero and incrementally rebuilding it exposed a transient false-idle
    // snapshot to status readers, which could trigger shutdown while the last
    // durable delivery was still awaiting its local ACK.
    let mut pending_counts = BTreeMap::<String, usize>::new();
    if let Some(outbox) = outbox {
        pending_counts = outbox.pending_counts();
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
    if let Some(outbox) = outbox {
        for (sink_tag, count) in outbox.pending_counts() {
            if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == &sink_tag) {
                stat.delivery.lock().expect("sink stats").durable_pending = count as u64;
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

fn fill_pending_from_outbox(
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    workers: &[SinkWorker],
    outbox: Option<&DeliveryOutbox>,
    limit: usize,
) {
    let Some(outbox) = outbox else {
        return;
    };
    let in_flight = workers
        .iter()
        .filter(|worker| worker.in_flight.is_some())
        .count();
    let room = effective_pending_limit(limit, workers.len())
        .saturating_sub(pending.len().saturating_add(in_flight));
    if room == 0 {
        return;
    }
    let mut excluded = pending
        .iter()
        .map(PendingDelivery::key)
        .collect::<HashSet<_>>();
    excluded.extend(workers.iter().filter_map(SinkWorker::in_flight_key));
    match outbox.load_pending_excluding(&excluded, room) {
        Ok(deliveries) => pending.extend(deliveries.into_iter().map(PendingDelivery::recovered)),
        Err(error) => {
            warn!(%error, "failed to page durable event deliveries into memory");
            let shared = stats.lock().expect("sink stats");
            for (_, stat) in shared.iter() {
                stat.record_failed(Some(error.to_string()));
            }
        }
    }
}

fn dispatch_event(
    workers: &[SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    blocked: &mut BlockedEvent,
    outbox: &mut Option<DeliveryOutbox>,
    dispatcher_config: EventDispatcherConfig,
) -> bool {
    for worker in workers {
        if blocked.persisted_sinks.contains(&worker.tag) || !worker.accepts(&blocked.event) {
            continue;
        }

        let prepared = worker.prepare_event(&blocked.event);

        // Throughput samples are deliberately lossy. They are useful while
        // the sink is healthy, but retaining every stale sample during an
        // outage would turn the outbox into an unbounded backlog.
        if is_discardable_sample(&blocked.event) {
            let delivery = PendingDelivery::new(worker.tag.clone(), prepared, false);
            blocked.persisted_sinks.insert(worker.tag.clone());
            enqueue_sample(
                pending,
                delivery,
                dispatcher_config.max_in_memory_deliveries,
            );
            continue;
        }

        let delivery = PendingDelivery::new(worker.tag.clone(), prepared, outbox.is_some());

        // Without an outbox, stop consuming source events once the bounded
        // in-memory workset is full. This keeps backpressure bounded instead
        // of allowing a slow sink to grow the process indefinitely.
        let pending_limit =
            effective_pending_limit(dispatcher_config.max_in_memory_deliveries, workers.len());
        if outbox.is_none() && pending.len() >= pending_limit {
            return false;
        }

        if let Err(error) = persist_delivery(outbox, &delivery) {
            if blocked.reported_failures.insert(worker.tag.clone()) {
                record_outbox_failure(stats, &worker.tag, &error);
            }
            blocked.defer_persistence_retry(dispatcher_config);
            return false;
        }
        blocked.persisted_sinks.insert(worker.tag.clone());
        if outbox.is_some()
            && pending.len()
                >= effective_pending_limit(
                    dispatcher_config.max_in_memory_deliveries,
                    workers.len(),
                )
        {
            continue;
        }

        pending.push_back(delivery);
    }
    true
}

fn is_discardable_sample(event: &RawApiEvent) -> bool {
    matches!(
        event.event_type.as_str(),
        event_type::FLOW_UPDATED | event_type::STATS_SAMPLED
    )
}

fn enqueue_sample(
    pending: &mut VecDeque<PendingDelivery>,
    delivery: PendingDelivery,
    max_in_memory_deliveries: usize,
) {
    if max_in_memory_deliveries == 0 {
        return;
    }
    if pending.len() < max_in_memory_deliveries {
        pending.push_back(delivery);
        return;
    }

    // Prefer evicting the oldest sample. If the queue contains only
    // lifecycle facts, drop the incoming sample instead.
    if let Some(index) = pending.iter().position(|queued| {
        queued.sink_tag == delivery.sink_tag && is_discardable_sample(&queued.event)
    }) {
        pending.remove(index);
        pending.push_back(delivery);
    }
}

fn effective_pending_limit(configured_limit: usize, worker_count: usize) -> usize {
    // An outbox-only configuration still needs one queued item per worker to
    // make progress. Durable backlog beyond that minimum remains on disk.
    configured_limit.max(worker_count.max(1))
}

fn submit_pending(
    workers: &mut [SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    dead_letter: Option<&DeadLetterSink>,
    outbox: &mut Option<DeliveryOutbox>,
    dispatcher_config: EventDispatcherConfig,
    allow_durable: bool,
) {
    let now = Instant::now();
    let len = pending.len();

    for _ in 0..len {
        let Some(delivery) = pending.pop_front() else {
            break;
        };

        if delivery.uses_outbox && !allow_durable {
            pending.push_back(delivery);
            continue;
        }

        if delivery.next_due > now {
            pending.push_back(delivery);
            continue;
        }

        if delivery.awaiting_ack {
            retry_ack(stats, pending, outbox, delivery, dispatcher_config);
            continue;
        }

        let Some(worker) = workers
            .iter_mut()
            .find(|worker| worker.tag == delivery.sink_tag)
        else {
            warn!(
                sink = %delivery.sink_tag,
                event_id = %delivery.event.event_id,
                "dropping pending event for missing sink"
            );
            if let Some(dl) = dead_letter {
                let _ = dl.publish(&delivery.event);
            }
            finish_delivery(stats, pending, outbox, delivery, dispatcher_config);
            continue;
        };

        if let Some(delivery) = worker.try_submit(delivery) {
            pending.push_back(delivery);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn drain_worker_results(
    workers: &mut [SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    max_attempts: u32,
    dead_letter: Option<&DeadLetterSink>,
    outbox: &mut Option<DeliveryOutbox>,
    dispatcher_config: EventDispatcherConfig,
    result_rx: &mut tokio::sync::mpsc::UnboundedReceiver<SinkWorkerResult>,
    completed_results: &mut VecDeque<SinkWorkerResult>,
) {
    while let Ok(completed) = result_rx.try_recv() {
        completed_results.push_back(completed);
    }
    while let Some(completed) = completed_results.pop_front() {
        let Some(worker) = workers
            .iter_mut()
            .find(|worker| worker.tag == completed.sink_tag)
        else {
            warn!(
                sink = %completed.sink_tag,
                event_id = %completed.event_id,
                "received event result for missing sink worker"
            );
            continue;
        };
        let Some(mut delivery) = worker.in_flight.take() else {
            warn!(
                sink = %completed.sink_tag,
                event_id = %completed.event_id,
                "received event result without an in-flight delivery"
            );
            continue;
        };
        if delivery.event.event_id != completed.event_id {
            warn!(
                sink = %completed.sink_tag,
                expected_event_id = %delivery.event.event_id,
                actual_event_id = %completed.event_id,
                "event sink worker returned a mismatched delivery"
            );
            delivery.next_due = Instant::now();
            pending.push_back(delivery);
            continue;
        }

        record_delivery(stats, &worker.tag, &completed.result);
        handle_publish_result(
            stats,
            pending,
            max_attempts,
            dead_letter,
            outbox,
            dispatcher_config,
            delivery,
            completed.result,
        );
    }
}

fn next_wait_interval(
    pending: &VecDeque<PendingDelivery>,
    workers: &[SinkWorker],
    blocked_events: &VecDeque<BlockedEvent>,
    source_poll_interval: Duration,
    allow_durable: bool,
) -> Duration {
    let now = Instant::now();
    let pending_due = pending
        .iter()
        .filter(|delivery| {
            (!delivery.uses_outbox || allow_durable)
                && (delivery.awaiting_ack
                    || workers.iter().any(|worker| {
                        worker.tag == delivery.sink_tag && worker.in_flight.is_none()
                    }))
        })
        .map(|delivery| delivery.next_due.saturating_duration_since(now))
        .min();
    let persistence_due = blocked_events
        .front()
        .and_then(|blocked| blocked.persistence_retry_due)
        .map(|due| due.saturating_duration_since(now));
    let source_poll_due = blocked_events.is_empty().then_some(source_poll_interval);

    pending_due
        .into_iter()
        .chain(persistence_due)
        .chain(source_poll_due)
        .min()
        .unwrap_or(source_poll_interval)
}

#[allow(clippy::too_many_arguments)]
fn handle_publish_result(
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    max_attempts: u32,
    dead_letter: Option<&DeadLetterSink>,
    outbox: &mut Option<DeliveryOutbox>,
    dispatcher_config: EventDispatcherConfig,
    mut delivery: PendingDelivery,
    result: SinkPublishResult,
) {
    match result {
        Ok(result) if result.delivered => {
            finish_delivery(stats, pending, outbox, delivery, dispatcher_config)
        }
        Ok(result)
            if result.retryable
                && (delivery.attempts < max_attempts
                    || dispatcher_config.exhausted_delivery_policy
                        == zero_config::ExhaustedDeliveryPolicy::RetryForever) =>
        {
            delivery.attempts = delivery.attempts.saturating_add(1);
            delivery.message = result.message;
            delivery.next_due = Instant::now() + retry_delay(delivery.attempts, dispatcher_config);
            let _ = persist_delivery(outbox, &delivery);
            pending.push_back(delivery);
        }
        Ok(result) => {
            warn!(
                sink = %delivery.sink_tag,
                event_id = %delivery.event.event_id,
                attempts = delivery.attempts,
                message = ?result.message,
                "event delivery reached its configured terminal state"
            );
            let should_dead_letter = !result.retryable
                || dispatcher_config.exhausted_delivery_policy
                    == zero_config::ExhaustedDeliveryPolicy::DeadLetter;
            if should_dead_letter {
                if let Some(dl) = dead_letter {
                    let _ = dl.publish(&delivery.event);
                }
            }
            finish_delivery(stats, pending, outbox, delivery, dispatcher_config);
        }
        Err(error)
            if delivery.attempts < max_attempts
                || dispatcher_config.exhausted_delivery_policy
                    == zero_config::ExhaustedDeliveryPolicy::RetryForever =>
        {
            delivery.attempts = delivery.attempts.saturating_add(1);
            delivery.message = Some(error.to_string());
            delivery.next_due = Instant::now() + retry_delay(delivery.attempts, dispatcher_config);
            let _ = persist_delivery(outbox, &delivery);
            pending.push_back(delivery);
        }
        Err(error) => {
            warn!(
                sink = %delivery.sink_tag,
                event_id = %delivery.event.event_id,
                attempts = delivery.attempts,
                error = %error,
                "event delivery reached its configured terminal state"
            );
            if dispatcher_config.exhausted_delivery_policy
                == zero_config::ExhaustedDeliveryPolicy::DeadLetter
            {
                if let Some(dl) = dead_letter {
                    let _ = dl.publish(&delivery.event);
                }
            }
            finish_delivery(stats, pending, outbox, delivery, dispatcher_config);
        }
    }
}

/// Record the outcome of a sink delivery into shared per-sink stats.
fn record_delivery(
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

fn now_unix_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn retry_delay(attempts: u32, config: EventDispatcherConfig) -> Duration {
    let exponent = attempts.saturating_sub(1).min(31);
    let delay_ms = config
        .retry_initial_delay_ms
        .saturating_mul(1_u64 << exponent)
        .min(config.retry_max_delay_ms);
    Duration::from_millis(delay_ms)
}

struct PendingDelivery {
    sink_tag: String,
    event: RawApiEvent,
    attempts: u32,
    next_due: Instant,
    message: Option<String>,
    awaiting_ack: bool,
    uses_outbox: bool,
}

struct BlockedEvent {
    event: RawApiEvent,
    persisted_sinks: HashSet<String>,
    reported_failures: HashSet<String>,
    persistence_attempts: u32,
    persistence_retry_due: Option<Instant>,
}

impl BlockedEvent {
    fn new(event: RawApiEvent) -> Self {
        Self {
            event,
            persisted_sinks: HashSet::new(),
            reported_failures: HashSet::new(),
            persistence_attempts: 0,
            persistence_retry_due: None,
        }
    }

    fn defer_persistence_retry(&mut self, config: EventDispatcherConfig) {
        self.persistence_attempts = self.persistence_attempts.saturating_add(1);
        self.persistence_retry_due =
            Some(Instant::now() + retry_delay(self.persistence_attempts, config));
    }
}

impl PendingDelivery {
    fn new(sink_tag: String, event: RawApiEvent, uses_outbox: bool) -> Self {
        Self {
            sink_tag,
            event,
            attempts: 0,
            next_due: Instant::now(),
            message: None,
            awaiting_ack: false,
            uses_outbox,
        }
    }

    fn recovered(delivery: OutboxDelivery) -> Self {
        Self {
            sink_tag: delivery.sink_tag,
            event: delivery.event,
            attempts: delivery.attempts,
            next_due: Instant::now(),
            message: delivery.message,
            awaiting_ack: false,
            uses_outbox: true,
        }
    }

    fn persisted(&self) -> OutboxDelivery {
        OutboxDelivery {
            sink_tag: self.sink_tag.clone(),
            event: self.event.clone(),
            attempts: self.attempts,
            message: self.message.clone(),
        }
    }

    fn key(&self) -> outbox::DeliveryKey {
        (self.sink_tag.clone(), self.event.event_id.clone())
    }
}

fn persist_delivery(
    outbox: &mut Option<DeliveryOutbox>,
    delivery: &PendingDelivery,
) -> ConnectorResult<()> {
    if !delivery.uses_outbox {
        return Ok(());
    }
    let Some(outbox) = outbox.as_mut() else {
        return Ok(());
    };
    if let Err(error) = outbox.put(&delivery.persisted()) {
        warn!(
            sink = %delivery.sink_tag,
            event_id = %delivery.event.event_id,
            error = %error,
            "failed to persist event delivery to outbox"
        );
        return Err(error);
    }
    Ok(())
}

fn ack_delivery(
    outbox: &mut Option<DeliveryOutbox>,
    delivery: &PendingDelivery,
) -> ConnectorResult<()> {
    if !delivery.uses_outbox {
        return Ok(());
    }
    let Some(outbox) = outbox.as_mut() else {
        return Ok(());
    };
    outbox.ack(&delivery.sink_tag, &delivery.event.event_id)
}

fn finish_delivery(
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    outbox: &mut Option<DeliveryOutbox>,
    mut delivery: PendingDelivery,
    config: EventDispatcherConfig,
) {
    if let Err(error) = ack_delivery(outbox, &delivery) {
        record_outbox_ack_failure(stats, &delivery.sink_tag, &error);
        delivery.awaiting_ack = true;
        delivery.attempts = delivery.attempts.saturating_add(1);
        delivery.message = Some(error.to_string());
        delivery.next_due = Instant::now() + retry_delay(delivery.attempts, config);
        pending.push_back(delivery);
    }
}

fn retry_ack(
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    outbox: &mut Option<DeliveryOutbox>,
    mut delivery: PendingDelivery,
    config: EventDispatcherConfig,
) {
    match ack_delivery(outbox, &delivery) {
        Ok(()) => record_outbox_ack_recovered(stats, &delivery.sink_tag),
        Err(error) => {
            record_outbox_ack_failure(stats, &delivery.sink_tag, &error);
            delivery.attempts = delivery.attempts.saturating_add(1);
            delivery.message = Some(error.to_string());
            delivery.next_due = Instant::now() + retry_delay(delivery.attempts, config);
            pending.push_back(delivery);
        }
    }
}

fn record_outbox_failure(stats: &SharedSinkStats, sink_tag: &str, error: &ConnectorError) {
    let shared = stats.lock().expect("sink stats");
    if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == sink_tag) {
        stat.record_failed(Some(error.to_string()));
    }
}

fn record_outbox_ack_failure(stats: &SharedSinkStats, sink_tag: &str, error: &ConnectorError) {
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

fn record_outbox_ack_recovered(stats: &SharedSinkStats, sink_tag: &str) {
    let shared = stats.lock().expect("sink stats");
    if let Some((_, stat)) = shared.iter().find(|(tag, _)| tag == sink_tag) {
        *stat.last_success_ms.lock().expect("sink stats") = Some(now_unix_ms());
        *stat.last_error.lock().expect("sink stats") = None;
    }
}

fn record_dispatcher_failure(stats: &SharedSinkStats, message: &str) {
    let shared = stats.lock().expect("sink stats");
    for (_, stat) in shared.iter() {
        stat.record_failed(Some(message.to_owned()));
    }
}

fn record_dispatcher_replay_gap(stats: &SharedSinkStats, message: &str) {
    let shared = stats.lock().expect("sink stats");
    for (_, stat) in shared.iter() {
        stat.replay_gaps.fetch_add(1, Ordering::Relaxed);
        stat.record_failed(Some(message.to_owned()));
    }
}
