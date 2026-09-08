//! Brutal's fixed-rate window and five-second packet-loss compensation.
//! Algorithm: Hysteria app/v2.12.2 core/internal/congestion/brutal (MIT).
use quinn::congestion::Controller;
use quinn_proto::RttEstimator;
use std::{
    any::Any,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Default)]
struct Slot {
    second: u64,
    acked: u64,
    lost: u64,
}
#[derive(Clone)]
pub(super) struct Brutal {
    epoch: Instant,
    slots: [Slot; 5],
    rate: Arc<AtomicU64>,
    ack_rate: f64,
    rtt: Duration,
    mtu: u16,
    disable_loss_compensation: bool,
}
impl Brutal {
    pub(super) fn new(
        now: Instant,
        mtu: u16,
        disable_loss_compensation: bool,
        rate: Arc<AtomicU64>,
    ) -> Self {
        Self {
            epoch: now,
            slots: [Slot::default(); 5],
            rate,
            ack_rate: 1.0,
            rtt: Duration::ZERO,
            mtu,
            disable_loss_compensation,
        }
    }
    fn sample(&mut self, now: Instant, acked: u64, lost: u64) {
        let second = now.saturating_duration_since(self.epoch).as_secs();
        let slot = &mut self.slots[(second % 5) as usize];
        if slot.second != second {
            *slot = Slot {
                second,
                ..Slot::default()
            };
        }
        slot.acked = slot.acked.saturating_add(acked);
        slot.lost = slot.lost.saturating_add(lost);
        let (acked, lost) = self
            .slots
            .iter()
            .filter(|s| s.second >= second.saturating_sub(5))
            .fold((0u64, 0u64), |(a, l), s| {
                (a.saturating_add(s.acked), l.saturating_add(s.lost))
            });
        self.ack_rate = if self.disable_loss_compensation || acked.saturating_add(lost) < 50 {
            1.0
        } else {
            (acked as f64 / (acked as f64 + lost as f64)).max(0.8)
        };
    }
}
impl Controller for Brutal {
    fn on_ack(&mut self, now: Instant, _: Instant, _: u64, _: bool, rtt: &RttEstimator) {
        self.rtt = rtt.get();
        self.sample(now, 1, 0);
    }
    fn on_packets_lost(&mut self, now: Instant, count: u64) {
        self.sample(now, 0, count);
    }
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, mtu: u16) {
        self.mtu = mtu;
    }
    fn window(&self) -> u64 {
        if self.rtt.is_zero() {
            10240
        } else {
            ((self.rate.load(Ordering::Relaxed) as f64 * self.rtt.as_secs_f64() * 2.0
                / self.ack_rate) as u64)
                .max(2 * u64::from(self.mtu))
        }
    }
    fn pacing_rate(&self) -> Option<u64> {
        Some(((self.rate.load(Ordering::Relaxed) as f64 / self.ack_rate) as u64).max(1))
    }
    fn initial_window(&self) -> u64 {
        10240
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[cfg(test)]
#[path = "../tests/brutal.rs"]
mod tests;
