use super::*;
impl Bbr {
    pub(super) fn update_pacing(&mut self, lost: u64) {
        let bandwidth = self.bandwidth.best();
        if bandwidth == 0 {
            return;
        }
        let target = (self.pacing_gain * bandwidth as f64) as u64;
        if self.full_bandwidth {
            self.pacing = target;
            return;
        }
        if self.pacing == 0 && !self.transport_min_rtt.is_zero() {
            self.pacing = sampler::rate(self.initial_window, self.transport_min_rtt);
            return;
        }
        if self.p.detect_overshooting {
            self.overshoot_lost += lost;
            if self.pacing > target
                && self.overshoot_lost > 0
                && (self.has_unlimited_sample
                    || self.overshoot_lost.saturating_mul(self.p.loss_multiplier)
                        > self.initial_window)
            {
                self.pacing = target.max(sampler::rate(
                    self.initial_window,
                    self.transport_min_rtt.max(Duration::from_nanos(1)),
                ));
                self.overshoot_lost = 0;
                self.p.detect_overshooting = false;
            }
        }
        self.pacing = self.pacing.max(target);
    }
    pub(super) fn update_window(&mut self, acked: u64, extra: u64) {
        if self.mode == Mode::ProbeRtt {
            return;
        }
        let target = self.target_window(self.window_gain)
            + if self.full_bandwidth {
                self.sampler.ack_height()
            } else if self.p.startup_ack_aggregation {
                extra
            } else {
                0
            };
        if self.full_bandwidth {
            self.cwnd = target.min(self.cwnd.saturating_add(acked));
        } else if self.cwnd < target || self.sampler.acked < self.initial_window {
            self.cwnd += acked;
        }
        self.cwnd = self.cwnd.clamp(self.minimum_window(), self.max_window);
    }
    pub(super) fn update_recovery_window(&mut self, acked: u64, lost: u64) {
        if self.recovery == Recovery::None {
            return;
        }
        if self.recovery_window == 0 {
            self.recovery_window = (self.flight + acked).max(self.minimum_window());
            return;
        }
        self.recovery_window = self.recovery_window.checked_sub(lost).unwrap_or(self.mtu);
        if self.recovery == Recovery::Growth {
            self.recovery_window += acked;
        }
        self.recovery_window = self
            .recovery_window
            .max(self.flight + acked)
            .max(self.minimum_window());
    }
}
