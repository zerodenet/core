use super::*;
impl Bbr {
    pub(super) fn feedback(&mut self, now: Instant, flight: u64) {
        if self.acks.is_empty() && self.losses.is_empty() {
            return;
        }
        let prior = flight
            + self.losses.iter().map(|(_, bytes)| bytes).sum::<u64>()
            + self
                .acks
                .iter()
                .filter_map(|key| self.sampler.packet_bytes(*key))
                .sum::<u64>();
        if prior < self.target_window(1.0) {
            self.sampler.mark_limited();
        }
        self.flight = flight;
        let largest = self
            .acks
            .iter()
            .filter_map(|key| self.sampler.sequence_for(*key))
            .max();
        let round_start = largest.is_some_and(|n| n > self.round_end);
        if round_start {
            self.round += 1;
            self.round_end = self.sampler.sequence;
        }
        let has_losses = !self.losses.is_empty();
        if let Some(largest) = largest {
            self.update_recovery(largest, has_losses, round_start);
        }
        let sample = self.sampler.feedback(
            now,
            &self.acks,
            &self.losses,
            self.bandwidth.best(),
            self.round,
        );
        self.acks.clear();
        self.losses.clear();
        if sample.last.valid {
            self.last_limited = sample.last.limited;
            self.has_unlimited_sample |= !sample.last.limited;
        }
        if sample.acked != 0 && (!sample.limited || sample.bandwidth > self.bandwidth.best()) {
            self.bandwidth.update(sample.bandwidth, self.round);
        }
        let mut expired = false;
        if let Some(rtt) = sample.rtt.filter(|rtt| !rtt.is_zero()) {
            expired = !self.min_rtt.is_zero()
                && now.saturating_duration_since(self.min_rtt_at) > Duration::from_secs(10);
            if expired || self.min_rtt.is_zero() || rtt < self.min_rtt {
                self.min_rtt = rtt;
                self.min_rtt_at = now;
            }
        }
        if has_losses {
            self.loss_events += 1;
            self.lost_in_round += sample.lost;
        }
        if self.mode == Mode::ProbeBw {
            self.update_cycle(now, prior, has_losses);
        }
        if round_start && !self.full_bandwidth && !self.last_limited {
            let target = (self.bandwidth_at_round as f64 * 1.25) as u64;
            if self.bandwidth.best() >= target {
                self.bandwidth_at_round = self.bandwidth.best();
                self.stalled_rounds = 0;
                if self.p.expire_startup_ack_aggregation {
                    self.sampler.reset_ack_height(self.round);
                }
            } else {
                self.stalled_rounds += 1;
                let excessive_loss = self.loss_events >= 8
                    && sample.last.valid
                    && sample.last.flight > 0
                    && self.lost_in_round > (sample.last.flight as f64 * 0.02) as u64;
                self.full_bandwidth =
                    self.stalled_rounds >= self.p.startup_rounds || excessive_loss;
            }
        }
        self.exit_startup_or_drain(now);
        self.probe_rtt(now, round_start, expired);
        self.update_pacing(sample.lost);
        self.update_window(sample.acked, sample.extra);
        self.update_recovery_window(sample.acked, sample.lost);
        if round_start {
            self.loss_events = 0;
            self.lost_in_round = 0;
        }
    }
}
