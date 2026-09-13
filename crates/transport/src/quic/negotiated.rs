//! A connection-local switch from an adaptive controller to negotiated Brutal.
use super::brutal::Brutal;
use quinn::congestion::{Controller, ControllerFactory};
use quinn_proto::RttEstimator;
use std::{
    any::Any,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};
pub struct Factory {
    pub adaptive: Arc<dyn ControllerFactory + Send + Sync>,
    pub disable_loss_compensation: bool,
    pub minimum_packets: u64,
}
impl ControllerFactory for Factory {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        let rate = Arc::new(AtomicU64::new(0));
        Box::new(NegotiatedController {
            adaptive: self.adaptive.clone().build(now, mtu),
            brutal: Brutal::new(
                now,
                mtu,
                self.disable_loss_compensation,
                rate.clone(),
                self.minimum_packets,
            ),
            rate,
        })
    }
}
struct NegotiatedController {
    adaptive: Box<dyn Controller>,
    brutal: Brutal,
    rate: Arc<AtomicU64>,
}
impl NegotiatedController {
    fn selected(&self) -> &dyn Controller {
        if self.rate.load(Ordering::Relaxed) == 0 {
            &*self.adaptive
        } else {
            &self.brutal
        }
    }
    fn selected_mut(&mut self) -> &mut dyn Controller {
        let rate = self.rate.load(Ordering::Relaxed);
        if rate == 0 {
            &mut *self.adaptive
        } else {
            &mut self.brutal
        }
    }
}
impl Controller for NegotiatedController {
    fn on_packet_event(&mut self, event: quinn_proto::congestion::PacketEvent) {
        self.selected_mut().on_packet_event(event);
    }
    fn on_sent(&mut self, now: Instant, bytes: u64, pn: u64) {
        self.selected_mut().on_sent(now, bytes, pn);
    }
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        limited: bool,
        rtt: &RttEstimator,
    ) {
        self.selected_mut().on_ack(now, sent, bytes, limited, rtt);
    }
    fn on_end_acks(&mut self, now: Instant, flight: u64, limited: bool, pn: Option<u64>) {
        self.selected_mut().on_end_acks(now, flight, limited, pn);
    }
    fn on_congestion_event(&mut self, now: Instant, sent: Instant, persistent: bool, lost: u64) {
        self.selected_mut()
            .on_congestion_event(now, sent, persistent, lost);
    }
    fn on_packets_lost(&mut self, now: Instant, count: u64) {
        self.selected_mut().on_packets_lost(now, count);
    }
    fn on_mtu_update(&mut self, mtu: u16) {
        self.adaptive.on_mtu_update(mtu);
        self.brutal.on_mtu_update(mtu);
    }
    fn window(&self) -> u64 {
        self.selected().window()
    }
    fn pacing_rate(&self) -> Option<u64> {
        self.selected().pacing_rate()
    }
    fn initial_window(&self) -> u64 {
        self.selected().initial_window()
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            adaptive: self.adaptive.clone_box(),
            brutal: self.brutal.clone(),
            rate: self.rate.clone(),
        })
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

pub fn negotiate(connection: &quinn::Connection, rate: u64) {
    if let Ok(controller) = connection
        .congestion_state()
        .into_any()
        .downcast::<NegotiatedController>()
    {
        controller.rate.store(rate, Ordering::Relaxed);
    }
}
