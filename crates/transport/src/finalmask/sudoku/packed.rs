use super::*;
pub struct PackedEncoder {
    profile: Profile,
    index: usize,
    chance: u32,
}
pub struct PackedDecoder {
    profile: Profile,
    index: usize,
    bits: u64,
    count: u8,
}
impl Profile {
    pub fn packed_encoder(&self) -> PackedEncoder {
        PackedEncoder {
            profile: self.clone(),
            index: 0,
            chance: rand::rng().random_range(self.padding_min..=self.padding_max),
        }
    }
    pub fn packed_decoder(&self) -> PackedDecoder {
        PackedDecoder {
            profile: self.clone(),
            index: 0,
            bits: 0,
            count: 0,
        }
    }
}
impl PackedEncoder {
    fn pad(&self, out: &mut Vec<u8>) {
        let mut rng = rand::rng();
        if rng.random_range(0..100) >= self.chance {
            return;
        }
        let table = &self.profile.tables[self.index];
        let marker = table.marker;
        loop {
            let b = table.padding[rng.random_range(0..table.padding.len())];
            if b != marker || table.padding.len() == 1 {
                out.push(b);
                return;
            }
        }
    }
    pub fn encode(&mut self, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(input.len() * 2);
        let mut bits = 0u64;
        let mut count = 0u8;
        for b in input {
            bits = (bits << 8) | u64::from(*b);
            count += 8;
            while count >= 6 {
                count -= 6;
                self.pad(&mut out);
                out.push(self.profile.tables[self.index].groups[((bits >> count) as usize) & 63]);
                self.index = (self.index + 1) % self.profile.tables.len();
                bits &= (1 << count) - 1;
            }
        }
        if count > 0 {
            self.pad(&mut out);
            out.push(self.profile.tables[self.index].groups[((bits << (6 - count)) as usize) & 63]);
            self.index = (self.index + 1) % self.profile.tables.len();
            out.push(self.profile.tables[self.index].marker);
        }
        self.pad(&mut out);
        out
    }
}
impl PackedDecoder {
    pub fn decode(&mut self, input: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(input.len());
        for b in input {
            let table = &self.profile.tables[self.index];
            let Some(group) = table.group_for[*b as usize] else {
                if *b == table.marker {
                    self.bits = 0;
                    self.count = 0;
                }
                continue;
            };
            self.index = (self.index + 1) % self.profile.tables.len();
            self.bits = (self.bits << 6) | u64::from(group);
            self.count += 6;
            while self.count >= 8 {
                self.count -= 8;
                out.push((self.bits >> self.count) as u8);
                self.bits &= (1 << self.count) - 1;
            }
        }
        out
    }
}
