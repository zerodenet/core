use super::*;
impl Sampler {
    pub(super) fn ack(&mut self, now: Instant, packet: Sent) -> Option<(u64, Duration)> {
        self.acked += packet.bytes;
        self.sent_at_last_ack = packet.state.sent;
        self.last_sent = Some(packet.time);
        self.last_ack = Some(now);
        if self.avoid_overestimate {
            let point = AckPoint {
                time: now,
                acked: self.acked,
            };
            if self.recent[1].is_none_or(|previous| now > previous.time) {
                self.recent[0] = self.recent[1];
            }
            self.recent[1] = Some(point);
        }
        if self.limited && packet.sequence > self.limited_end {
            self.limited = false;
        }
        let last_sent = packet.last_sent?;
        let send_rate = if packet.time > last_sent {
            rate(
                packet.state.sent - packet.sent_at_last_ack,
                packet.time - last_sent,
            )
        } else {
            u64::MAX
        };
        let point = if self.avoid_overestimate {
            self.choose_a0(packet.state.acked)
        } else {
            None
        }
        .or(packet.last_ack.map(|time| AckPoint {
            time,
            acked: packet.state.acked,
        }))?;
        if now <= point.time {
            return None;
        }
        Some((
            send_rate.min(rate(
                self.acked.saturating_sub(point.acked),
                now - point.time,
            )),
            now.saturating_duration_since(packet.time),
        ))
    }
    fn choose_a0(&mut self, acked: u64) -> Option<AckPoint> {
        let index = self
            .candidates
            .iter()
            .position(|p| p.acked > acked)
            .map(|i| i.saturating_sub(1))
            .unwrap_or(self.candidates.len().saturating_sub(1));
        for _ in 0..index {
            self.candidates.pop_front();
        }
        self.candidates.front().copied()
    }
}
