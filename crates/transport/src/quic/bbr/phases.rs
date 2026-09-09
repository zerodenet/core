use super::*;
const GAINS: [f64; 8] = [1.25, 0.75, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0];
impl Bbr {
    fn enter_startup(&mut self) {
        self.mode = Mode::Startup;
        self.pacing_gain = self.p.startup_pacing_gain;
        self.window_gain = self.p.startup_window_gain;
    }
    fn enter_probe_bw(&mut self, now: Instant) {
        use rand::Rng;
        self.mode = Mode::ProbeBw;
        self.window_gain = self.p.window_gain;
        self.cycle = rand::rng().random_range(0..7);
        if self.cycle >= 1 {
            self.cycle += 1;
        }
        self.cycle_start = now;
        self.pacing_gain = GAINS[self.cycle];
    }
    pub(super) fn update_cycle(&mut self, now: Instant, prior: u64, lost: bool) {
        let mut advance = now.saturating_duration_since(self.cycle_start) > self.rtt();
        if self.pacing_gain > 1.0 && !lost && prior < self.target_window(self.pacing_gain) {
            advance = false;
        }
        if self.pacing_gain < 1.0 && self.flight <= self.target_window(1.0) {
            advance = true;
        }
        if advance {
            self.cycle = (self.cycle + 1) % 8;
            self.cycle_start = now;
            if self.p.drain_to_target
                && self.pacing_gain < 1.0
                && GAINS[self.cycle] == 1.0
                && self.flight > self.target_window(1.0)
            {
                return;
            }
            self.pacing_gain = GAINS[self.cycle];
        }
    }
    pub(super) fn exit_startup_or_drain(&mut self, now: Instant) {
        if self.mode == Mode::Startup && self.full_bandwidth {
            self.mode = Mode::Drain;
            self.pacing_gain = 1.0 / self.p.startup_pacing_gain;
            self.window_gain = self.p.startup_window_gain;
        }
        if self.mode == Mode::Drain && self.flight <= self.target_window(1.0) {
            self.enter_probe_bw(now);
        }
    }
    pub(super) fn probe_rtt(&mut self, now: Instant, round_start: bool, expired: bool) {
        if expired && !self.exiting_idle && self.mode != Mode::ProbeRtt {
            self.mode = Mode::ProbeRtt;
            self.pacing_gain = 1.0;
            self.probe_exit = None;
        }
        if self.mode == Mode::ProbeRtt {
            self.sampler.mark_limited();
            match self.probe_exit {
                None if self.flight < self.minimum_window() + 1452 => {
                    self.probe_exit = Some(now + Duration::from_millis(200));
                    self.probe_round = false;
                }
                Some(exit) => {
                    self.probe_round |= round_start;
                    if now >= exit && self.probe_round {
                        self.min_rtt_at = now;
                        if self.full_bandwidth {
                            self.enter_probe_bw(now);
                        } else {
                            self.enter_startup();
                        }
                    }
                }
                _ => {}
            }
        }
        self.exiting_idle = false;
    }
    pub(super) fn update_recovery(&mut self, largest: u64, lost: bool, round_start: bool) {
        if !self.full_bandwidth {
            return;
        }
        if lost {
            self.recovery_end = self.sampler.sequence;
        }
        match self.recovery {
            Recovery::None if lost => {
                self.recovery = Recovery::Conservation;
                self.recovery_window = 0;
                self.round_end = self.sampler.sequence;
            }
            Recovery::Conservation | Recovery::Growth => {
                if self.recovery == Recovery::Conservation && round_start {
                    self.recovery = Recovery::Growth;
                }
                if !lost && largest > self.recovery_end {
                    self.recovery = Recovery::None;
                }
            }
            _ => {}
        }
    }
}
