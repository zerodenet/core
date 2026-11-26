use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use tracing::{debug, warn};
use zero_api::EventSource;
use zero_config::EventDispatcherConfig;

use super::*;

#[allow(clippy::too_many_arguments)]
pub(in crate::dispatcher) async fn run_event_dispatcher<S>(
    source: source::Source<S>,
    subscriber: source::Subscriber<S::Stream>,
    baseline: Option<u64>,
    source_budget: Arc<source::SourceBudget>,
    bootstrap_events: Vec<BlockedEvent>,
    sinks: Vec<ConfiguredEventSink>,
    stats: &SharedSinkStats,
    options: EventDispatcherOptions,
    mut shutdown: tokio::sync::oneshot::Receiver<ShutdownMode>,
    dead_letter: Option<Arc<RetainedDeadLetter>>,
    outbox: &mut Option<DeliveryOutbox>,
    dispatcher_config: EventDispatcherConfig,
    core_instance_id: String,
    startup_sequence: u64,
    continuation: Option<ContinuationState>,
    init: InitSignal,
    mut activation: Option<tokio::sync::oneshot::Receiver<()>>,
    children: Arc<DispatcherChildren>,
) -> EventDispatcherContinuation
where
    S: EventSource + Send + Sync + 'static,
{
    let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut workers = sinks
        .into_iter()
        .map(|sink| SinkWorker::spawn(sink, result_tx.clone(), children.clone()))
        .collect::<Vec<_>>();
    drop(result_tx);
    let source_cancelled = Arc::new(AtomicBool::new(false));
    children
        .reserve_source()
        .unwrap_or_else(|error| std::panic::panic_any(error));
    let (mut source_rx, source_budget, source_task, start_source) = source::spawn_bridge(
        subscriber,
        source_cancelled.clone(),
        dispatcher_config.replay_batch_size,
        dispatcher_config.source_queue_max_bytes,
        source_budget,
    )
    .unwrap_or_else(|error| {
        children.source_not_started();
        std::panic::panic_any(error)
    });
    children.source(source_task, source_cancelled.clone(), source_budget.clone());
    let _ = start_source.send(());
    for (_, sink) in stats.lock().expect("sink stats").iter() {
        *sink.source_buffer.lock().expect("sink stats") = Some(source_budget.clone());
    }
    let delivery_budget =
        memory::DeliveryBudget::new(dispatcher_config.max_in_memory_delivery_bytes);
    for (_, sink) in stats.lock().expect("sink stats").iter() {
        *sink.delivery_buffer.lock().expect("sink stats") = Some(delivery_budget.clone());
    }
    let mut pending = VecDeque::new();
    refresh_pending_stats(stats, &pending, &workers, outbox.as_ref());
    let mut last_sequence = None;
    let mut blocked_events = bootstrap_events.into_iter().collect::<VecDeque<_>>();
    let inherited = continuation.is_some();
    if let Some(mut state) = continuation {
        last_sequence = state.last_sequence;
        pending.append(&mut state.deliveries);
        if let Some(mut blocked) = state.blocked {
            blocked.persistence_retry_due = None;
            blocked.prepared = None;
            blocked_events.push_front(blocked);
        }
    }
    for delivery in &mut pending {
        delivery.reservation = Some(delivery_budget.inherit(delivery.byte_charge));
    }
    let mut initial_baseline = if inherited { None } else { baseline };
    let mut source_events = VecDeque::new();
    let mut completed_results = VecDeque::new();
    let mut source_closed = false;
    let max_retry_attempts = if options.max_retry_attempts == 0 {
        dispatcher_config.max_retry_attempts
    } else {
        options.max_retry_attempts
    };

    let mut activated = activation.is_none();
    let _ = init.send(Ok(true));
    let mut shutting_down = false;
    let mut handing_off = false;
    loop {
        drain_worker_results(
            &mut workers,
            stats,
            &mut pending,
            max_retry_attempts,
            dead_letter.as_deref(),
            outbox,
            dispatcher_config,
            &mut result_rx,
            &mut completed_results,
        );
        if activated && !shutting_down {
            fill_pending_from_outbox(
                stats,
                &mut pending,
                &workers,
                outbox.as_ref(),
                dispatcher_config.max_in_memory_deliveries,
                &delivery_budget,
            );
        }
        if activated && !handing_off {
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
                &delivery_budget,
                &source_budget,
                &mut initial_baseline,
            );
            submit_pending(
                &mut workers,
                stats,
                &mut pending,
                dead_letter.as_deref(),
                outbox,
                dispatcher_config,
                !shutting_down,
            );
        }
        refresh_pending_stats(stats, &pending, &workers, outbox.as_ref());
        source_budget.set_paused(!blocked_events.is_empty());

        if shutting_down {
            let has_in_flight = workers.iter().any(|worker| worker.in_flight.is_some());
            let has_ready = pending
                .iter()
                .any(|delivery| !delivery.uses_outbox && delivery.next_due <= Instant::now());
            if source_closed
                && !has_in_flight
                && (handing_off || (!has_ready && (outbox.is_some() || blocked_events.is_empty())))
            {
                break;
            }
        }

        let wait_interval = if handing_off || !activated {
            options.poll_interval
        } else {
            next_wait_interval(
                &pending,
                &workers,
                &blocked_events,
                options.poll_interval,
                !shutting_down,
            )
        };
        tokio::select! {
            biased;
            ready = async { activation.as_mut().expect("pending publication receiver").await }, if activation.is_some() && !shutting_down => {
                if ready.is_ok() { activated = true; }
                activation = None;
            }
            mode = &mut shutdown, if !shutting_down => {
                shutting_down = true;
                handing_off = !activated || matches!(mode, Ok(ShutdownMode::Handoff));
                source_cancelled.store(true, Ordering::Release);
                for worker in &mut workers {
                    if let Some(delivery) = worker.cancel_shutdown_in_flight(outbox.is_some(), handing_off) {
                        pending.push_back(delivery);
                    }
                }
                while !handing_off && drain_event_source(
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
                    &delivery_budget,
                    &source_budget,
                    &mut initial_baseline,
                ) {}
                if handing_off || (outbox.is_some() && !blocked_events.is_empty()) {
                    source_rx.close();
                    source_budget.close();
                    source_closed = true;
                    warn!(
                        event_id = ?blocked_events.front().map(|blocked| &blocked.event.event_id),
                        deferred_events = blocked_events.len(),
                        "event dispatcher stopped with events that could not be persisted; the event cursor was not advanced"
                    );
                }
            }
            Some(result) = result_rx.recv() => completed_results.push_back(result),
            event = source_rx.recv(), if activated && !source_closed && blocked_events.is_empty() && source_events.is_empty() => match event {
                Some(event) => source_events.push_back(event),
                None => source_closed = true,
            },
            _ = tokio::time::sleep(wait_interval) => {}
        }
    }

    source_cancelled.store(true, Ordering::Release);
    source_budget.close();
    source_rx.close();
    for worker in &mut workers {
        worker.stop();
    }
    for worker in &mut workers {
        let _ = worker.join().await;
    }
    children.join_source();
    drain_worker_results(
        &mut workers,
        stats,
        &mut pending,
        max_retry_attempts,
        dead_letter.as_deref(),
        outbox,
        dispatcher_config,
        &mut result_rx,
        &mut completed_results,
    );
    refresh_pending_stats(stats, &pending, &workers, outbox.as_ref());

    debug!("event dispatcher stopped");
    EventDispatcherContinuation::new(ContinuationState {
        core_instance_id,
        last_sequence: last_sequence.or_else(|| {
            Some(
                blocked_events
                    .front()
                    .and_then(|event| event.event.sequence)
                    .map(|sequence| sequence.saturating_sub(1))
                    .unwrap_or(startup_sequence),
            )
        }),
        blocked: blocked_events.pop_front(),
        deliveries: pending
            .into_iter()
            .filter(|delivery| !delivery.uses_outbox)
            .filter(|delivery| !is_discardable_sample(&delivery.event))
            .collect(),
    })
}
