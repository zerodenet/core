//! Shared BBR delivery model and state machine, independent of HY2 negotiation.
//!
//! Based on Hysteria app/v2.12.2 (619a6f856b69fb7ee6a7a379e810e68b84004605)
//! core/internal/congestion/bbr. Copyright 2023 Toby; MIT, see LICENSE-HYSTERIA.
//! Packet scheduling, ACK generation and path recovery remain Quinn-owned.
mod config;
mod controller;
mod feedback;
mod filter;
mod phases;
mod sampler;
mod window;
pub use config::{BbrConfig, BbrParameters};
use filter::Filter;
use quinn_proto::congestion::PacketKey;
use sampler::Sampler;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Startup,
    Drain,
    ProbeBw,
    ProbeRtt,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Recovery {
    None,
    Conservation,
    Growth,
}
#[derive(Clone)]
pub struct Bbr {
    p: BbrParameters,
    sampler: Sampler,
    bandwidth: Filter<u64>,
    mode: Mode,
    recovery: Recovery,
    mtu: u64,
    initial_window: u64,
    max_window: u64,
    cwnd: u64,
    recovery_window: u64,
    flight: u64,
    min_rtt: Duration,
    transport_min_rtt: Duration,
    min_rtt_at: Instant,
    pacing: u64,
    pacing_gain: f64,
    window_gain: f64,
    round: u64,
    round_end: u64,
    full_bandwidth: bool,
    bandwidth_at_round: u64,
    stalled_rounds: u64,
    recovery_end: u64,
    loss_events: u64,
    lost_in_round: u64,
    overshoot_lost: u64,
    last_limited: bool,
    has_unlimited_sample: bool,
    exiting_idle: bool,
    cycle: usize,
    cycle_start: Instant,
    probe_exit: Option<Instant>,
    probe_round: bool,
    acks: Vec<PacketKey>,
    losses: Vec<(PacketKey, u64)>,
}
impl Bbr {
    fn new(p: BbrParameters, initial_window: u64, now: Instant, mtu: u16) -> Self {
        let mtu = u64::from(mtu);
        Self {
            p,
            sampler: Sampler::new(p),
            bandwidth: Filter::new(|v| v),
            mode: Mode::Startup,
            recovery: Recovery::None,
            mtu,
            initial_window,
            max_window: 20_000 * mtu,
            cwnd: initial_window,
            recovery_window: 20_000 * mtu,
            flight: 0,
            min_rtt: Duration::ZERO,
            transport_min_rtt: Duration::ZERO,
            min_rtt_at: now,
            pacing: 0,
            pacing_gain: p.startup_pacing_gain,
            window_gain: p.startup_window_gain,
            round: 0,
            round_end: 0,
            full_bandwidth: false,
            bandwidth_at_round: 0,
            stalled_rounds: 0,
            recovery_end: 0,
            loss_events: 0,
            lost_in_round: 0,
            overshoot_lost: 0,
            last_limited: false,
            has_unlimited_sample: false,
            exiting_idle: false,
            cycle: 0,
            cycle_start: now,
            probe_exit: None,
            probe_round: false,
            acks: Vec::new(),
            losses: Vec::new(),
        }
    }
    fn rtt(&self) -> Duration {
        if !self.min_rtt.is_zero() {
            self.min_rtt
        } else if !self.transport_min_rtt.is_zero() {
            self.transport_min_rtt
        } else {
            Duration::from_millis(100)
        }
    }
    fn minimum_window(&self) -> u64 {
        4 * self.mtu
    }
    fn target_window(&self, gain: f64) -> u64 {
        let bdp = sampler::delivered(self.bandwidth.best(), self.rtt());
        let target = (bdp as f64 * gain) as u64;
        (if target == 0 {
            (self.initial_window as f64 * gain) as u64
        } else {
            target
        })
        .max(self.minimum_window())
    }
    fn effective_window(&self) -> u64 {
        if self.mode == Mode::ProbeRtt {
            self.minimum_window()
        } else if self.recovery != Recovery::None {
            self.cwnd.min(self.recovery_window)
        } else {
            self.cwnd
        }
    }
    fn effective_pacing(&self) -> u64 {
        let bits = if self.pacing == 0 {
            (sampler::rate(self.initial_window, self.rtt()) as f64 * self.p.startup_pacing_gain)
                as u64
        } else {
            self.pacing
        };
        (bits / 8).max(65_536)
    }
}
#[cfg(test)]
mod tests;
