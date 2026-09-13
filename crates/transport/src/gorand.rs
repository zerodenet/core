//! Go 1 math/rand compatibility for the pinned browser version schedule.
//! Not used for cryptography. Algorithm and constants: Go Authors, BSD-3-Clause
//! (see gorand/LICENSE). Signed arithmetic intentionally wraps like Go int64.
mod cooked;
pub(crate) struct GoRandom {
    tap: usize,
    feed: usize,
    words: [i64; 607],
}
impl GoRandom {
    pub(crate) fn new(seed: i64) -> Self {
        let mut x = seed.rem_euclid(i32::MAX as i64);
        if x == 0 {
            x = 89482311;
        }
        fn step(x: i64) -> i64 {
            (48271 * x).rem_euclid(i32::MAX as i64)
        }
        let mut words = [0; 607];
        for i in -20i32..607 {
            x = step(x);
            if i >= 0 {
                let mut word = x.wrapping_shl(40);
                x = step(x);
                word ^= x.wrapping_shl(20);
                x = step(x);
                word ^= x;
                words[i as usize] = word ^ cooked::COOKED[i as usize];
            }
        }
        Self {
            tap: 0,
            feed: 607 - 273,
            words,
        }
    }
    fn int63(&mut self) -> u64 {
        self.tap = (self.tap + 606) % 607;
        self.feed = (self.feed + 606) % 607;
        let word = self.words[self.feed].wrapping_add(self.words[self.tap]);
        self.words[self.feed] = word;
        word as u64 & 0x7fff_ffff_ffff_ffff
    }
    pub(crate) fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            let n = (i + 1) as u32;
            let threshold = n.wrapping_neg() % n;
            let chosen = loop {
                let value = (self.int63() >> 31) as u32;
                let product = u64::from(value) * u64::from(n);
                if product as u32 >= threshold {
                    break (product >> 32) as usize;
                }
            };
            values.swap(i, chosen);
        }
    }
    #[allow(dead_code)] // Used by optional browser-profile carriers.
    pub(crate) fn below(&mut self, n: u32) -> u32 {
        let max = i32::MAX as u32 - (1u32 << 31) % n;
        loop {
            let value = (self.int63() >> 32) as u32;
            if value <= max {
                return value % n;
            }
        }
    }
}
