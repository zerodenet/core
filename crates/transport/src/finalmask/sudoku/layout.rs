use std::io;
#[derive(Clone)]
pub(super) enum Layout {
    Entropy,
    Ascii,
    Custom { x: [u8; 2], p: [u8; 2], v: [u8; 4] },
}
impl Layout {
    pub(super) fn new(ascii: &str, custom: &str) -> io::Result<Self> {
        match ascii.trim().to_ascii_lowercase().as_str() {
            "ascii" | "prefer_ascii" => return Ok(Self::Ascii),
            "" | "entropy" | "prefer_entropy" => {}
            _ => return Err(super::invalid("invalid Sudoku ASCII mode")),
        }
        let custom = custom.trim().to_ascii_lowercase().replace(' ', "");
        if custom.is_empty() {
            return Ok(Self::Entropy);
        }
        if custom.len() != 8 {
            return Err(super::invalid("Sudoku custom table must have eight bits"));
        }
        let mut x = Vec::new();
        let mut p = Vec::new();
        let mut v = Vec::new();
        for (i, kind) in custom.bytes().enumerate() {
            let bit = 7 - i as u8;
            match kind {
                b'x' => x.push(bit),
                b'p' => p.push(bit),
                b'v' => v.push(bit),
                _ => return Err(super::invalid("invalid Sudoku custom table bit")),
            }
        }
        Ok(Self::Custom {
            x: x.try_into()
                .map_err(|_| super::invalid("Sudoku requires two x bits"))?,
            p: p.try_into()
                .map_err(|_| super::invalid("Sudoku requires two p bits"))?,
            v: v.try_into()
                .map_err(|_| super::invalid("Sudoku requires four v bits"))?,
        })
    }
    pub(super) fn encode(&self, group: u8) -> u8 {
        let group = group & 63;
        match self {
            Self::Entropy => ((group & 0x30) << 1) | (group & 15),
            Self::Ascii => {
                if group == 63 {
                    b'\n'
                } else {
                    0x40 | group
                }
            }
            Self::Custom { x, p, v } => {
                let mut b = (1 << x[0]) | (1 << x[1]);
                for (i, bit) in p.iter().chain(v).enumerate() {
                    if group & (1 << (5 - i)) != 0 {
                        b |= 1 << bit;
                    }
                }
                b
            }
        }
    }
    pub(super) fn decode(&self, b: u8) -> Option<u8> {
        match self {
            Self::Entropy => (b & 0x90 == 0).then_some(((b >> 1) & 0x30) | (b & 15)),
            Self::Ascii => {
                if b == b'\n' {
                    Some(63)
                } else {
                    (b & 0x40 != 0).then_some(b & 63)
                }
            }
            Self::Custom { x, p, v } => {
                let mask = (1 << x[0]) | (1 << x[1]);
                if b & mask != mask {
                    return None;
                }
                let mut group = 0;
                for (i, bit) in p.iter().chain(v).enumerate() {
                    if b & (1 << bit) != 0 {
                        group |= 1 << (5 - i);
                    }
                }
                Some(group)
            }
        }
    }
    pub(super) fn padding(&self) -> Vec<u8> {
        match self {
            Self::Ascii => (0x20..0x40).collect(),
            Self::Entropy => (0..8).flat_map(|i| [0x80 + i, 0x10 + i]).collect(),
            Self::Custom { x, .. } => {
                let mut values = Vec::new();
                for bit in x {
                    for group in 0..64 {
                        let b = self.encode(group) & !(1 << bit);
                        if b.count_ones() >= 5 {
                            values.push(b);
                        }
                    }
                }
                values.sort_unstable();
                values.dedup();
                values
            }
        }
    }
    pub(super) fn marker(&self) -> u8 {
        match self {
            Self::Ascii => 0x3f,
            Self::Entropy => 0x80,
            _ => self.padding()[0],
        }
    }
}
