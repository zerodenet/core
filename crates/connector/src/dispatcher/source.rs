//! Event ingestion and replay, independent of sink transport.
use super::*;

#[allow(clippy::too_many_arguments)]
pub(super) fn drain_event_source<S>(
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
