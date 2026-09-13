use super::wire::Body;
use bytes::Bytes;
use std::collections::HashMap;
struct Ack {
    number: u32,
    timestamp: u32,
    flush: u32,
}
pub(super) struct Receiver {
    pub next: u32,
    window: u32,
    packets: HashMap<u32, Bytes>,
    acks: Vec<Ack>,
    dirty: bool,
}
impl Receiver {
    pub fn new(window: u32) -> Self {
        Self {
            next: 0,
            window,
            packets: HashMap::new(),
            acks: Vec::new(),
            dirty: false,
        }
    }
    pub fn clear(&mut self, una: u32) {
        let before = self.acks.len();
        self.acks.retain(|ack| ack.number >= una);
        self.dirty |= before != self.acks.len();
    }
    pub fn input(&mut self, timestamp: u32, number: u32, sending_next: u32, payload: Bytes) {
        if number.wrapping_sub(self.next) >= self.window {
            return;
        }
        self.clear(sending_next);
        // Cumulative receiving-next acknowledges older delivered packets too.
        // Retain one window behind it instead of letting a stale peer UNA grow
        // an unbounded acknowledgement list.
        self.acks.retain(|ack| {
            self.next.wrapping_sub(ack.number) <= self.window
                || ack.number.wrapping_sub(self.next) < self.window
        });
        // A duplicate only refreshes its existing ACK, bounding memory even
        // when the sender retransmits aggressively before advancing UNA.
        if let Some(ack) = self.acks.iter_mut().find(|ack| ack.number == number) {
            ack.timestamp = timestamp;
            ack.flush = 0;
        } else {
            self.acks.push(Ack {
                number,
                timestamp,
                flush: 0,
            });
        }
        self.dirty = true;
        self.packets.entry(number).or_insert(payload);
    }
    pub fn front(&self) -> Option<Bytes> {
        self.packets.get(&self.next).cloned()
    }
    pub fn consume(&mut self) {
        self.packets.remove(&self.next);
        self.next = self.next.wrapping_add(1);
        self.dirty = true;
    }
    pub fn flush(&mut self, now: u32, rto: u32, mtu: usize) -> Vec<Body> {
        let limit = ((mtu - 17) / 4).clamp(1, 128);
        let mut pending = Vec::new();
        let mut candidates = Vec::new();
        for ack in &mut self.acks {
            if ack.flush > now {
                if candidates.len() < 128 {
                    candidates.push(ack.number);
                }
                continue;
            }
            pending.push((ack.number, ack.timestamp));
            ack.flush = now.wrapping_add((rto / 2).max(20));
        }
        let mut frames = Vec::new();
        for chunk in pending.chunks(limit) {
            let mut numbers: Vec<_> = chunk.iter().map(|v| v.0).collect();
            if numbers.len() < limit {
                numbers.extend(candidates.iter().copied().take(limit - numbers.len()));
            }
            let timestamp = chunk.iter().map(|v| v.1).max().unwrap_or(0);
            frames.push(Body::Ack {
                window: self.next.wrapping_add(self.window),
                next: self.next,
                timestamp,
                numbers,
            });
        }
        if frames.is_empty() && self.dirty {
            frames.push(Body::Ack {
                window: self.next.wrapping_add(self.window),
                next: self.next,
                timestamp: 0,
                numbers: candidates.into_iter().take(limit).collect(),
            });
        }
        self.dirty = false;
        frames
    }
}
