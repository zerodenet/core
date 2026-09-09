use super::*;
use quinn::congestion::{Controller, ControllerMetrics};
use quinn_proto::congestion::PacketEvent;
use std::any::Any;
impl Controller for Bbr {
    fn on_packet_event(&mut self, event: PacketEvent) {
        match event {
            PacketEvent::Sent {
                key,
                now,
                bytes,
                in_flight,
                ack_eliciting,
            } => {
                self.exiting_idle |= in_flight == 0;
                self.sampler.send(key, now, bytes, in_flight, ack_eliciting);
            }
            PacketEvent::Acked { key } => self.acks.push(key),
            PacketEvent::Lost { key, bytes } => self.losses.push((key, bytes)),
            PacketEvent::Discarded { key } => self.sampler.discard(key),
            PacketEvent::DiscardSpace { space } => self.sampler.discard_space(space),
            PacketEvent::FeedbackEnd {
                now,
                in_flight,
                min_rtt,
            } => {
                self.transport_min_rtt = min_rtt;
                self.feedback(now, in_flight);
            }
        }
    }
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, mtu: u16) {
        let mtu = u64::from(mtu);
        let old_min = self.minimum_window();
        let old_initial = self.initial_window;
        self.initial_window = self.initial_window * mtu / self.mtu;
        self.max_window = self.max_window * mtu / self.mtu;
        self.mtu = mtu;
        self.cwnd = if self.cwnd == old_min {
            self.minimum_window()
        } else if self.cwnd == old_initial {
            self.initial_window
        } else {
            self.cwnd.clamp(self.minimum_window(), self.max_window)
        };
        self.recovery_window = self
            .recovery_window
            .clamp(self.minimum_window(), self.max_window);
    }
    fn window(&self) -> u64 {
        self.effective_window()
    }
    fn pacing_rate(&self) -> Option<u64> {
        Some(self.effective_pacing())
    }
    fn initial_window(&self) -> u64 {
        self.initial_window
    }
    fn metrics(&self) -> ControllerMetrics {
        let mut metrics = ControllerMetrics::default();
        metrics.congestion_window = self.effective_window();
        metrics.pacing_rate = Some(self.effective_pacing().saturating_mul(8));
        metrics
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
