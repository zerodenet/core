use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tracing::warn;
use zero_api::EventSink;
use zero_config::EventDispatcherConfig;

use super::*;

pub(in crate::dispatcher) fn submit_pending(
    workers: &mut [SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    dead_letter: Option<&RetainedDeadLetter>,
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

        if !allow_durable && !delivery.uses_outbox && is_discardable_sample(&delivery.event) {
            continue;
        }

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
pub(in crate::dispatcher) fn drain_worker_results(
    workers: &mut [SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    max_attempts: u32,
    dead_letter: Option<&RetainedDeadLetter>,
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

pub(in crate::dispatcher) fn next_wait_interval(
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
    dead_letter: Option<&RetainedDeadLetter>,
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
