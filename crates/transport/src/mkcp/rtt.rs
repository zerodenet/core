#[derive(Debug)]
pub(super) struct RoundTrip {
    pub rto: u32,
    variation: u32,
    smoothed: u32,
    minimum: u32,
    updated: u32,
}
impl RoundTrip {
    pub fn new(minimum: u32) -> Self {
        Self {
            rto: 100,
            variation: 0,
            smoothed: 0,
            minimum,
            updated: 0,
        }
    }
    pub fn peer(&mut self, rto: u32, now: u32) {
        if now.wrapping_sub(self.updated) >= 3000 {
            self.updated = now;
            self.rto = rto;
        }
    }
    pub fn update(&mut self, rtt: u32, now: u32) {
        if rtt > 0x7fff_ffff {
            return;
        }
        if self.smoothed == 0 {
            self.smoothed = rtt;
            self.variation = rtt / 2;
        } else {
            self.variation = (3 * self.variation + self.smoothed.abs_diff(rtt)) / 4;
            self.smoothed = ((7 * self.smoothed + rtt) / 8).max(self.minimum);
        }
        let rto = self.smoothed
            + if self.minimum < 4 * self.variation {
                4 * self.variation
            } else {
                self.variation
            };
        self.rto = rto.min(10000) * 5 / 4;
        self.updated = now;
    }
}
