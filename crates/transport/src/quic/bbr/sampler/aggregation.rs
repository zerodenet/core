use super::*;
#[derive(Clone, Copy, Default)]
pub(super) struct Height {
    extra: u64,
    bytes: u64,
    delta: Duration,
    round: u64,
}
#[derive(Clone)]
pub(super) struct Aggregation {
    filter: Filter<Height>,
    start: Option<Instant>,
    bytes: u64,
    threshold: f64,
    reduce_on_growth: bool,
}
impl Aggregation {
    pub fn new(p: BbrParameters) -> Self {
        Self {
            filter: Filter::new(|h: Height| h.extra),
            start: None,
            bytes: 0,
            threshold: if p.avoid_overestimate { 2.0 } else { 1.0 },
            reduce_on_growth: p.reduce_ack_height_on_bandwidth_growth,
        }
    }
    pub fn best(&self) -> u64 {
        self.filter.best().extra
    }
    pub fn reset(&mut self, round: u64) {
        self.filter.reset(Height::default(), round);
    }
    pub fn update(
        &mut self,
        now: Instant,
        acked: u64,
        bandwidth: u64,
        grew: bool,
        round: u64,
    ) -> u64 {
        if grew && self.reduce_on_growth {
            let old = self.filter.entries;
            self.filter = Filter::new(|h: Height| h.extra);
            for entry in old {
                let mut h = entry.value;
                let expected = delivered(bandwidth, h.delta);
                if expected < h.bytes {
                    h.extra = h.bytes - expected;
                    self.filter.update(h, h.round);
                }
            }
        }
        let delta = self.start.map(|start| now.saturating_duration_since(start));
        let expected = delta.map_or(0, |delta| delivered(bandwidth, delta));
        if delta.is_none() || self.bytes <= (expected as f64 * self.threshold) as u64 {
            self.bytes = acked;
            self.start = Some(now);
            return 0;
        }
        self.bytes += acked;
        let extra = self.bytes.saturating_sub(expected);
        // The pinned reference leaves the event's round field at its zero value.
        self.filter.update(
            Height {
                extra,
                bytes: self.bytes,
                delta: delta.unwrap(),
                round: 0,
            },
            round,
        );
        extra
    }
}

#[cfg(test)]
#[path = "../tests/aggregation.rs"]
mod tests;
