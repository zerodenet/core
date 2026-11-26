use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::warn;
use zero_api::RawApiEvent;
use zero_config::EventDispatcherConfig;

use super::*;

pub(in crate::dispatcher) struct PendingDelivery {
    pub(in crate::dispatcher) sink_tag: String,
    pub(in crate::dispatcher) event: Arc<RawApiEvent>,
    pub(in crate::dispatcher) byte_charge: u64,
    pub(in crate::dispatcher) reservation: Option<memory::DeliveryReservation>,
    pub(in crate::dispatcher) attempts: u32,
    pub(in crate::dispatcher) next_due: Instant,
    pub(in crate::dispatcher) message: Option<String>,
    pub(in crate::dispatcher) awaiting_ack: bool,
    pub(in crate::dispatcher) uses_outbox: bool,
}

pub(in crate::dispatcher) struct BlockedEvent {
    pub(in crate::dispatcher) prepared: Option<admission::PreparedInput>,
    pub(in crate::dispatcher) event: Arc<RawApiEvent>,
    pub(in crate::dispatcher) source_reservation: Option<source::SourceReservation>,
    pub(in crate::dispatcher) persisted_sinks: HashSet<String>,
    pub(in crate::dispatcher) reported_failures: HashSet<String>,
    pub(in crate::dispatcher) persistence_attempts: u32,
    pub(in crate::dispatcher) persistence_retry_due: Option<Instant>,
}

impl BlockedEvent {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(in crate::dispatcher) fn new(event: RawApiEvent) -> Self {
        Self::from_shared(Arc::new(event))
    }

    pub(in crate::dispatcher) fn from_shared(event: Arc<RawApiEvent>) -> Self {
        Self {
            prepared: None,
            event,
            source_reservation: None,
            persisted_sinks: HashSet::new(),
            reported_failures: HashSet::new(),
            persistence_attempts: 0,
            persistence_retry_due: None,
        }
    }

    pub(in crate::dispatcher) fn defer_persistence_retry(&mut self, config: EventDispatcherConfig) {
        self.persistence_attempts = self.persistence_attempts.saturating_add(1);
        self.persistence_retry_due =
            Some(Instant::now() + retry_delay(self.persistence_attempts, config));
    }
}

impl PendingDelivery {
    #[cfg(test)]
    pub(in crate::dispatcher) fn new(
        sink_tag: String,
        event: impl Into<Arc<RawApiEvent>>,
        uses_outbox: bool,
    ) -> Self {
        let event = event.into();
        let byte_charge = memory::serialized_bytes(&*event).max(1);
        Self::with_charge(sink_tag, event, uses_outbox, byte_charge)
    }

    pub(in crate::dispatcher) fn with_charge(
        sink_tag: String,
        event: Arc<RawApiEvent>,
        uses_outbox: bool,
        byte_charge: u64,
    ) -> Self {
        Self {
            sink_tag,
            event,
            byte_charge,
            reservation: None,
            attempts: 0,
            next_due: Instant::now(),
            message: None,
            awaiting_ack: false,
            uses_outbox,
        }
    }

    #[cfg(test)]
    pub(in crate::dispatcher) fn recovered(delivery: OutboxDelivery) -> Self {
        let bytes = memory::serialized_bytes(&delivery.event).max(1);
        Self::recovered_with_charge(delivery, bytes)
    }

    pub(in crate::dispatcher) fn recovered_with_charge(
        delivery: OutboxDelivery,
        bytes: u64,
    ) -> Self {
        Self {
            sink_tag: delivery.sink_tag,
            byte_charge: bytes.max(1),
            reservation: None,
            event: Arc::new(delivery.event),
            attempts: delivery.attempts,
            next_due: Instant::now(),
            message: delivery.message,
            awaiting_ack: false,
            uses_outbox: true,
        }
    }

    pub(in crate::dispatcher) fn key(&self) -> outbox::DeliveryKey {
        (self.sink_tag.clone(), self.event.event_id.clone())
    }
}

pub(in crate::dispatcher) fn retry_delay(attempts: u32, config: EventDispatcherConfig) -> Duration {
    let exponent = attempts.saturating_sub(1).min(31);
    let delay_ms = config
        .retry_initial_delay_ms
        .saturating_mul(1_u64 << exponent)
        .min(config.retry_max_delay_ms);
    Duration::from_millis(delay_ms)
}

pub(in crate::dispatcher) fn persist_delivery(
    outbox: &mut Option<DeliveryOutbox>,
    delivery: &PendingDelivery,
) -> ConnectorResult<()> {
    if !delivery.uses_outbox {
        return Ok(());
    }
    let Some(outbox) = outbox.as_mut() else {
        return Ok(());
    };
    if let Err(error) = outbox.put_ref(
        &delivery.sink_tag,
        &delivery.event,
        delivery.attempts,
        delivery.message.as_deref(),
    ) {
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

pub(in crate::dispatcher) fn ack_delivery(
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

pub(in crate::dispatcher) fn finish_delivery(
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

pub(in crate::dispatcher) fn retry_ack(
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
