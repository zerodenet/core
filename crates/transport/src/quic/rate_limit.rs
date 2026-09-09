//! Connection-wide pacing ceiling, independent of the protocol and controller.
use quinn::congestion::{Controller, ControllerFactory, ControllerMetrics};
use quinn_proto::RttEstimator;
use std::{any::Any, sync::Arc, time::Instant};

pub(super) fn cap_factory(
    inner: Arc<dyn ControllerFactory + Send + Sync>,
    max_rate: Option<u64>,
) -> Arc<dyn ControllerFactory + Send + Sync> {
    match max_rate.filter(|rate| *rate > 0) {
        Some(max_rate) => Arc::new(CappedFactory { inner, max_rate }),
        None => inner,
    }
}

struct CappedFactory {
    inner: Arc<dyn ControllerFactory + Send + Sync>,
    max_rate: u64,
}

impl ControllerFactory for CappedFactory {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        Box::new(CappedController {
            inner: self.inner.clone().build(now, mtu),
            max_rate: self.max_rate,
        })
    }
}

struct CappedController {
    inner: Box<dyn Controller>,
    max_rate: u64,
}

impl Controller for CappedController {
    fn on_packet_event(&mut self, event: quinn_proto::congestion::PacketEvent) {
        self.inner.on_packet_event(event);
    }
    fn on_sent(&mut self, now: Instant, bytes: u64, pn: u64) {
        self.inner.on_sent(now, bytes, pn);
    }

    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        limited: bool,
        rtt: &RttEstimator,
    ) {
        self.inner.on_ack(now, sent, bytes, limited, rtt);
    }

    fn on_end_acks(&mut self, now: Instant, flight: u64, limited: bool, pn: Option<u64>) {
        self.inner.on_end_acks(now, flight, limited, pn);
    }

    fn on_congestion_event(&mut self, now: Instant, sent: Instant, persistent: bool, lost: u64) {
        self.inner.on_congestion_event(now, sent, persistent, lost);
    }

    fn on_packets_lost(&mut self, now: Instant, count: u64) {
        self.inner.on_packets_lost(now, count);
    }

    fn on_mtu_update(&mut self, mtu: u16) {
        self.inner.on_mtu_update(mtu);
    }

    fn window(&self) -> u64 {
        self.inner.window()
    }

    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }

    fn pacing_rate(&self) -> Option<u64> {
        Some(
            self.inner
                .pacing_rate()
                .unwrap_or(self.max_rate)
                .min(self.max_rate),
        )
    }

    fn metrics(&self) -> ControllerMetrics {
        let mut metrics = self.inner.metrics();
        metrics.pacing_rate = self.pacing_rate().map(|rate| rate.saturating_mul(8));
        metrics
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            max_rate: self.max_rate,
        })
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        // Preserve controller-specific negotiation/state handles. The live
        // connection still owns its capped wrapper when a snapshot is read.
        self.inner.into_any()
    }
}

#[cfg(test)]
#[path = "tests/rate_limit.rs"]
mod tests;
