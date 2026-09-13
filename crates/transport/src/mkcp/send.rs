use super::{config::Settings, wire::Body};
use bytes::Bytes;
use std::collections::VecDeque;
struct Pending {
    number: u32,
    payload: Bytes,
    deadline: u32,
    transmits: u32,
}
pub(super) struct Sender {
    settings: Settings,
    queue: VecDeque<Pending>,
    next: u32,
    remote_window: u32,
    control_window: u32,
    una_changed: bool,
}
impl Sender {
    pub fn new(settings: Settings) -> Self {
        Self {
            settings,
            queue: VecDeque::new(),
            next: 0,
            remote_window: 32,
            control_window: settings.send_window(),
            una_changed: false,
        }
    }
    pub fn can_push(&self) -> bool {
        self.queue.len() < self.settings.send_buffer()
    }
    pub fn push(&mut self, payload: Bytes) {
        self.queue.push_back(Pending {
            number: self.next,
            payload,
            deadline: 0,
            transmits: 0,
        });
        self.next = self.next.wrapping_add(1);
    }
    pub fn una(&self) -> u32 {
        self.queue.front().map_or(self.next, |packet| packet.number)
    }
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }
    pub fn clear(&mut self, next: u32) {
        if after(next, self.next) {
            return;
        }
        let previous = self.una();
        self.queue.retain(|packet| !before(packet.number, next));
        self.una_changed |= previous != self.una();
    }
    pub fn close(&mut self) {
        self.queue.clear();
    }
    pub fn ack(
        &mut self,
        window: u32,
        next: u32,
        timestamp: u32,
        numbers: &[u32],
        now: u32,
        rto: u32,
    ) -> Option<u32> {
        if after(window, self.remote_window) {
            self.remote_window = window;
        }
        self.clear(next);
        let previous = self.una();
        let mut max_ack = None;
        for &number in numbers {
            if let Some(index) = self.queue.iter().position(|packet| packet.number == number) {
                self.queue.remove(index);
                if max_ack.is_none_or(|last| after(number, last)) {
                    max_ack = Some(number);
                }
            }
        }
        self.una_changed |= previous != self.una();
        if let Some(number) = max_ack {
            for packet in &mut self.queue {
                if !before(packet.number, number) {
                    break;
                }
                if packet.transmits > 0 && packet.deadline > rto / 3 {
                    packet.deadline -= rto / 3;
                }
            }
            let elapsed = now.wrapping_sub(timestamp);
            if elapsed < 10000 {
                return Some(elapsed);
            }
        }
        None
    }
    pub fn flush(&mut self, now: u32, rto: u32) -> (Vec<Body>, bool) {
        let una = self.una();
        let mut window = self
            .settings
            .send_window()
            .min(self.remote_window.wrapping_sub(una));
        if self.settings.congestion {
            window = window.min(self.control_window);
        }
        let max_frames = window.saturating_mul(20).max(1) as usize;
        let mut sent = Vec::new();
        let mut lost = 0;
        for packet in &mut self.queue {
            if now.wrapping_sub(packet.deadline) >= 0x7fff_ffff {
                continue;
            }
            if packet.transmits > 0 {
                lost += 1;
            }
            packet.transmits = packet.transmits.saturating_add(1);
            packet.deadline = now.wrapping_add(rto);
            sent.push(Body::Data {
                timestamp: now,
                number: packet.number,
                sending_next: una,
                payload: packet.payload.clone(),
            });
            if sent.len() >= max_frames {
                break;
            }
        }
        if self.settings.congestion && rto != 0 && !sent.is_empty() {
            let in_flight = self
                .queue
                .iter()
                .filter(|packet| packet.transmits > 0)
                .count()
                .max(1);
            let loss = lost * 100 / in_flight;
            if loss >= 15 {
                self.control_window = 3 * self.control_window / 4;
            } else if loss <= 5 {
                self.control_window += self.control_window / 4;
            }
            self.control_window = self
                .control_window
                .max(16)
                .min(2 * self.settings.send_window());
        }
        let ping = self.queue.is_empty() && self.una_changed;
        self.una_changed = false;
        (sent, ping)
    }
}
fn before(a: u32, b: u32) -> bool {
    (a.wrapping_sub(b) as i32) < 0
}
fn after(a: u32, b: u32) -> bool {
    before(b, a)
}
