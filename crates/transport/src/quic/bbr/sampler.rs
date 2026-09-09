//! Delivery-rate samples capture connection state at each packet's send time.
use super::{filter::Filter, BbrParameters};
use quinn_proto::congestion::PacketKey;
use std::{
    collections::{BTreeMap, VecDeque},
    time::{Duration, Instant},
};
mod ack;
mod aggregation;
use aggregation::Aggregation;

#[derive(Clone, Copy, Default)]
pub(super) struct SendState {
    pub valid: bool,
    pub limited: bool,
    pub sent: u64,
    pub acked: u64,
    pub flight: u64,
}
#[derive(Clone, Copy)]
pub(super) struct Sent {
    pub sequence: u64,
    pub time: Instant,
    pub bytes: u64,
    pub state: SendState,
    last_sent: Option<Instant>,
    last_ack: Option<Instant>,
    sent_at_last_ack: u64,
}
#[derive(Clone, Copy)]
struct AckPoint {
    time: Instant,
    acked: u64,
}
#[derive(Default)]
pub(super) struct Sample {
    pub bandwidth: u64,
    pub rtt: Option<Duration>,
    pub limited: bool,
    pub last: SendState,
    pub extra: u64,
    pub acked: u64,
    pub lost: u64,
}
#[derive(Clone)]
pub(super) struct Sampler {
    packets: BTreeMap<PacketKey, Sent>,
    pub sent: u64,
    pub acked: u64,
    pub lost: u64,
    pub sequence: u64,
    limited_end: u64,
    limited: bool,
    last_sent: Option<Instant>,
    last_ack: Option<Instant>,
    sent_at_last_ack: u64,
    avoid_overestimate: bool,
    recent: [Option<AckPoint>; 2],
    candidates: VecDeque<AckPoint>,
    aggregation: Aggregation,
}
impl Sampler {
    pub fn new(p: BbrParameters) -> Self {
        Self {
            packets: BTreeMap::new(),
            sent: 0,
            acked: 0,
            lost: 0,
            sequence: 0,
            limited_end: 0,
            limited: false,
            last_sent: None,
            last_ack: None,
            sent_at_last_ack: 0,
            avoid_overestimate: p.avoid_overestimate,
            recent: [None; 2],
            candidates: VecDeque::new(),
            aggregation: Aggregation::new(p),
        }
    }
    pub fn ack_height(&self) -> u64 {
        self.aggregation.best()
    }
    pub fn reset_ack_height(&mut self, round: u64) {
        self.aggregation.reset(round);
    }
    pub fn mark_limited(&mut self) {
        self.limited = true;
        self.limited_end = self.sequence;
    }
    pub fn send(&mut self, key: PacketKey, now: Instant, bytes: u64, flight: u64, eliciting: bool) {
        self.sequence += 1;
        if !eliciting {
            return;
        }
        self.sent += bytes;
        if flight == 0 {
            self.last_ack = Some(now);
            self.last_sent = Some(now);
            self.sent_at_last_ack = self.sent;
            if self.avoid_overestimate {
                let point = AckPoint {
                    time: now,
                    acked: self.acked,
                };
                self.recent = [None, Some(point)];
                self.candidates.clear();
                self.candidates.push_back(point);
            }
        }
        self.packets.insert(
            key,
            Sent {
                sequence: self.sequence,
                time: now,
                bytes,
                state: SendState {
                    valid: true,
                    limited: self.limited,
                    sent: self.sent,
                    acked: self.acked,
                    flight: flight + bytes,
                },
                last_sent: self.last_sent,
                last_ack: self.last_ack,
                sent_at_last_ack: self.sent_at_last_ack,
            },
        );
    }
    pub fn discard(&mut self, key: PacketKey) {
        self.packets.remove(&key);
    }
    pub fn discard_space(&mut self, space: u8) {
        self.packets.retain(|key, _| key.0 != space);
    }
    pub fn sequence_for(&self, key: PacketKey) -> Option<u64> {
        self.packets.get(&key).map(|p| p.sequence)
    }
    pub fn packet_bytes(&self, key: PacketKey) -> Option<u64> {
        self.packets.get(&key).map(|p| p.bytes)
    }
    pub fn feedback(
        &mut self,
        now: Instant,
        acks: &[PacketKey],
        losses: &[(PacketKey, u64)],
        max_bw: u64,
        round: u64,
    ) -> Sample {
        let before = self.acked;
        let mut result = Sample::default();
        let mut last_sequence = 0;
        for &(key, bytes) in losses {
            self.lost += bytes;
            result.lost += bytes;
            if let Some(packet) = self.packets.remove(&key) {
                if packet.sequence >= last_sequence {
                    last_sequence = packet.sequence;
                    result.last = packet.state;
                }
            }
        }
        for key in acks {
            if let Some(packet) = self.packets.remove(key) {
                if let Some((bandwidth, rtt)) = self.ack(now, packet) {
                    if !rtt.is_zero() {
                        result.rtt = Some(result.rtt.map_or(rtt, |old| old.min(rtt)));
                    }
                    if bandwidth > result.bandwidth {
                        result.bandwidth = bandwidth;
                        result.limited = packet.state.limited;
                    }
                    if packet.sequence >= last_sequence {
                        last_sequence = packet.sequence;
                        result.last = packet.state;
                    }
                }
            }
        }
        result.acked = self.acked - before;
        if result.acked > 0 {
            result.extra = self.aggregation.update(
                now,
                result.acked,
                max_bw.max(result.bandwidth),
                result.bandwidth > max_bw,
                round,
            );
            if self.avoid_overestimate && result.extra == 0 {
                if let Some(point) = self.recent[0].filter(|p| p.acked != 0).or(self.recent[1]) {
                    self.candidates.push_back(point);
                }
            }
        }
        result
    }
}
pub(super) fn rate(bytes: u64, duration: Duration) -> u64 {
    if duration.is_zero() {
        return u64::MAX;
    }
    ((bytes as u128 * 1_000_000_000 / duration.as_nanos()) * 8).min(u64::MAX as u128) as u64
}
pub(super) fn delivered(bandwidth: u64, duration: Duration) -> u64 {
    (bandwidth as u128 * duration.as_nanos() / 8_000_000_000).min(u64::MAX as u128) as u64
}
