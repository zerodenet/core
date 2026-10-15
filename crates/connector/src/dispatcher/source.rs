//! Event ingestion and replay, independent of sink transport.
use super::*;

mod bridge;
mod budget;
pub(super) use bridge::{spawn as spawn_bridge, QueuedSourceEvent};
pub(super) use budget::{SourceBudget, SourceReservation};

#[allow(clippy::too_many_arguments)]
pub(super) fn drain_event_source<S>(
    source: &S,
    source_rx: &mut tokio::sync::mpsc::Receiver<QueuedSourceEvent>,
    source_events: &mut VecDeque<QueuedSourceEvent>,
    workers: &[SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    outbox: &mut Option<DeliveryOutbox>,
    last_sequence: &mut Option<u64>,
    blocked_events: &mut VecDeque<BlockedEvent>,
    dispatcher_config: EventDispatcherConfig,
    budget: &Arc<memory::DeliveryBudget>,
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
            && !dispatch_event(
                workers,
                stats,
                pending,
                blocked,
                outbox,
                dispatcher_config,
                budget,
            )
        {
            return false;
        }
        if let Some(sequence) = blocked.event.sequence {
            *last_sequence = Some(sequence);
        }
        blocked_events.pop_front();
        progressed = true;
    }

    // The batch setting bounds work per pass, not a batch of owned payloads.
    // Recover one event at a time so a large replay cannot inflate staging.
    for _ in 0..dispatcher_config.replay_batch_size {
        let mut blocked = source_events
            .pop_front()
            .or_else(|| source_rx.try_recv().ok())
            .map(QueuedSourceEvent::into_blocked);
        if blocked.as_ref().is_some_and(|event| {
            last_sequence.is_some_and(|last| {
                event
                    .event
                    .sequence
                    .is_some_and(|next| next > last.saturating_add(1))
            })
        }) {
            // Release the future live event's reservation before recovering
            // its missing prefix. Its identity remains in the source history.
            blocked = None;
        }
        if blocked.is_none() {
            let Some(sequence) = *last_sequence else {
                break;
            };
            match source.since(sequence, 1, EventFilter::default()) {
                Ok(mut replay) => {
                    if replay.has_gap {
                        let message = format!(
                            "event replay gap after sequence {}; retained events start at {}",
                            replay.requested_after, replay.actual_from
                        );
                        record_dispatcher_replay_gap(stats, &message);
                        warn!(requested_after = replay.requested_after, actual_from = replay.actual_from,
                            "event dispatcher replay gap detected; older sink deliveries require reconciliation");
                        *last_sequence = Some(replay.actual_from.saturating_sub(1));
                    }
                    blocked = replay.events.pop().map(BlockedEvent::new);
                }
                Err(error) => {
                    record_dispatcher_failure(stats, &error.to_string());
                    warn!(%error, "failed to replay dispatcher events");
                    break;
                }
            }
        }
        let Some(mut blocked) = blocked else { break };
        if let Some(sequence) = blocked.event.sequence {
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
        if blocked.event.event_type != event_type::FLOW_SNAPSHOT {
            if !dispatch_event(
                workers,
                stats,
                pending,
                &mut blocked,
                outbox,
                dispatcher_config,
                budget,
            ) {
                blocked_events.push_back(blocked);
                return progressed;
            }
            if let Some(sequence) = blocked.event.sequence {
                *last_sequence = Some(sequence);
            }
        } else if let Some(sequence) = blocked.event.sequence {
            *last_sequence = Some(sequence);
        }
        progressed = true;
    }
    progressed
}
