//! Working-set admission and durable paging; never changes Engine accounting.
use super::*;

pub(super) fn fill_pending_from_outbox(
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    workers: &[SinkWorker],
    outbox: Option<&DeliveryOutbox>,
    limit: usize,
    budget: &Arc<memory::DeliveryBudget>,
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
    match outbox.load_pending_page(&excluded, room, budget.available(), budget.is_empty()) {
        Ok(page) => {
            budget.set_paging_blocked(page.waiting_for_bytes);
            for (delivery, bytes) in page.deliveries {
                let mut delivery = PendingDelivery::recovered_with_charge(delivery, bytes);
                delivery.reservation = Some(budget.inherit(bytes));
                pending.push_back(delivery);
            }
        }
        Err(error) => {
            warn!(%error, "failed to page durable event deliveries into memory");
            let shared = stats.lock().expect("sink stats");
            for (_, stat) in shared.iter() {
                stat.record_failed(Some(error.to_string()));
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn dispatch_event(
    workers: &[SinkWorker],
    stats: &SharedSinkStats,
    pending: &mut VecDeque<PendingDelivery>,
    blocked: &mut BlockedEvent,
    outbox: &mut Option<DeliveryOutbox>,
    dispatcher_config: EventDispatcherConfig,
    budget: &Arc<memory::DeliveryBudget>,
) -> bool {
    for worker in workers {
        if blocked.persisted_sinks.contains(&worker.tag) || !worker.accepts(&blocked.event) {
            continue;
        }

        let (prepared, byte_charge) = blocked.prepare_for(worker);

        // Throughput samples are deliberately lossy. They are useful while
        // the sink is healthy, but retaining every stale sample during an
        // outage would turn the outbox into an unbounded backlog.
        if is_discardable_sample(&blocked.event) {
            if budget.paging_blocked() {
                blocked.persisted_sinks.insert(worker.tag.clone());
                continue;
            }
            let mut delivery =
                PendingDelivery::with_charge(worker.tag.clone(), prepared, false, byte_charge);
            // Samples may replace only other volatile samples. Release that
            // old charge before attempting the replacement reservation.
            if !budget.can_reserve(delivery.byte_charge)
                || pending.len() + workers.iter().filter(|w| w.in_flight.is_some()).count()
                    >= dispatcher_config.max_in_memory_deliveries
            {
                if let Some(index) = pending.iter().position(|queued| {
                    !queued.uses_outbox
                        && queued.sink_tag == worker.tag
                        && is_discardable_sample(&queued.event)
                }) {
                    pending.remove(index);
                }
            }
            let Some(reservation) = budget.reserve(delivery.byte_charge) else {
                blocked.persisted_sinks.insert(worker.tag.clone());
                continue;
            };
            delivery.reservation = Some(reservation);
            blocked.persisted_sinks.insert(worker.tag.clone());
            enqueue_sample(
                pending,
                delivery,
                dispatcher_config
                    .max_in_memory_deliveries
                    .saturating_sub(workers.iter().filter(|w| w.in_flight.is_some()).count()),
            );
            continue;
        }

        let mut delivery = PendingDelivery::with_charge(
            worker.tag.clone(),
            prepared,
            outbox.is_some(),
            byte_charge,
        );

        // Without an outbox, stop consuming source events once the bounded
        // in-memory workset is full. This keeps backpressure bounded instead
        // of allowing a slow sink to grow the process indefinitely.
        let pending_limit =
            effective_pending_limit(dispatcher_config.max_in_memory_deliveries, workers.len());
        if outbox.is_none()
            && (pending.len() + workers.iter().filter(|w| w.in_flight.is_some()).count()
                >= pending_limit
                || !budget.can_reserve(delivery.byte_charge))
        {
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
        if budget.paging_blocked() {
            // Drain the current working set so an older oversized durable
            // entry can enter alone. Live traffic must not starve it forever.
            continue;
        }
        if outbox.is_some()
            && pending.len() + workers.iter().filter(|w| w.in_flight.is_some()).count()
                >= effective_pending_limit(
                    dispatcher_config.max_in_memory_deliveries,
                    workers.len(),
                )
        {
            continue;
        }

        let Some(reservation) = budget.reserve(delivery.byte_charge) else {
            // Durable identity remains indexed; paging resumes when room is free.
            continue;
        };
        delivery.reservation = Some(reservation);
        pending.push_back(delivery);
    }
    true
}

pub(super) fn is_discardable_sample<P>(event: &zero_api::ApiEvent<P>) -> bool {
    matches!(
        event.event_type.as_str(),
        event_type::FLOW_UPDATED
            | event_type::STATS_SAMPLED
            | "stats.scopes_sampled"
            | "stats.host_interfaces_sampled"
            | "endpoint.stats_sampled"
    )
}

pub(super) fn enqueue_sample(
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

    // Only newly admitted volatile samples are replaceable. Legacy samples
    // already in the journal keep their delivery/ACK working-set position.
    if let Some(index) = pending.iter().position(|queued| {
        !queued.uses_outbox
            && queued.sink_tag == delivery.sink_tag
            && is_discardable_sample(&queued.event)
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

// Keep only the current sink-specific input, outside the queued/in-flight
// workset. Repeated admission polls must not deep-clone or re-encode a large
// payload while a source_id override waits for storage or memory room.
pub(super) struct PreparedInput {
    sink_tag: String,
    event: Arc<RawApiEvent>,
    byte_charge: u64,
}
impl BlockedEvent {
    pub(super) fn prepare_for(&mut self, worker: &SinkWorker) -> (Arc<RawApiEvent>, u64) {
        if !self
            .prepared
            .as_ref()
            .is_some_and(|p| p.sink_tag == worker.tag)
        {
            let event = worker.prepare_event(&self.event);
            let byte_charge = memory::serialized_bytes(&*event).max(1);
            self.prepared = Some(PreparedInput {
                sink_tag: worker.tag.clone(),
                event,
                byte_charge,
            });
        }
        let prepared = self.prepared.as_ref().expect("prepared input");
        (prepared.event.clone(), prepared.byte_charge)
    }
}
