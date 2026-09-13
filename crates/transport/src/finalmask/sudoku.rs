//! Sudoku appearance transform, including per-byte table rotation.
//! Wire algorithm: Xray v26.3.27, transport/internet/finalmask/sudoku (MPL-2.0).
use rand::{seq::SliceRandom, Rng};
use std::{io, sync::Arc};
mod layout;
mod packed;
mod table;
pub use packed::{PackedDecoder, PackedEncoder};
#[derive(Debug, Clone, Default)]
pub struct Settings {
    pub password: String,
    pub ascii: String,
    pub custom_tables: Vec<String>,
    pub padding_min: u32,
    pub padding_max: u32,
}
#[derive(Clone)]
pub struct Profile {
    tables: Arc<[table::Table]>,
    padding_min: u32,
    padding_max: u32,
}
impl Profile {
    pub fn new(settings: &Settings) -> io::Result<Self> {
        let mut patterns = settings.custom_tables.clone();
        if patterns.is_empty() {
            patterns.push(String::new());
        }
        if matches!(
            settings.ascii.trim().to_ascii_lowercase().as_str(),
            "ascii" | "prefer_ascii"
        ) {
            patterns = vec![String::new()];
        }
        let mut normalized = Vec::new();
        for pattern in patterns {
            let p = pattern.trim().to_ascii_lowercase().replace(' ', "");
            if !normalized.contains(&p) {
                normalized.push(p);
            }
        }
        if normalized.len() > 64 {
            return Err(invalid("too many Sudoku tables"));
        }
        let tables = normalized
            .iter()
            .map(|p| {
                layout::Layout::new(&settings.ascii, p)
                    .map(|layout| table::Table::new(&settings.password, layout))
            })
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            tables: tables.into(),
            padding_min: settings.padding_min.min(100),
            padding_max: settings
                .padding_max
                .min(100)
                .max(settings.padding_min.min(100)),
        })
    }
    pub fn encoder(&self) -> Encoder {
        Encoder {
            profile: self.clone(),
            index: 0,
            chance: rand::rng().random_range(self.padding_min..=self.padding_max),
        }
    }
    pub fn decoder(&self) -> Decoder {
        Decoder {
            profile: self.clone(),
            index: 0,
            hints: Vec::with_capacity(4),
        }
    }
    pub fn encode_datagram(&self, input: &[u8]) -> Vec<u8> {
        self.encoder().encode(input)
    }
    pub fn decode_datagram(&self, input: &[u8]) -> io::Result<Vec<u8>> {
        let mut decoder = self.decoder();
        let out = decoder.decode(input)?;
        if !decoder.hints.is_empty() {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        Ok(out)
    }
}
pub struct Encoder {
    profile: Profile,
    index: usize,
    chance: u32,
}
impl Encoder {
    pub fn encode(&mut self, input: &[u8]) -> Vec<u8> {
        if input.is_empty() {
            return Vec::new();
        }
        let mut rng = rand::rng();
        let mut out = Vec::with_capacity(input.len() * 6);
        for byte in input {
            let table = &self.profile.tables[self.index];
            if rng.random_range(0..100) < self.chance {
                out.push(table.padding[rng.random_range(0..table.padding.len())]);
            }
            let choices = &table.encode[*byte as usize];
            let mut hints = choices[rng.random_range(0..choices.len())];
            hints.shuffle(&mut rng);
            for hint in hints {
                if rng.random_range(0..100) < self.chance {
                    out.push(table.padding[rng.random_range(0..table.padding.len())]);
                }
                out.push(hint);
            }
            self.index = (self.index + 1) % self.profile.tables.len();
        }
        if rng.random_range(0..100) < self.chance {
            let table = &self.profile.tables[self.index];
            out.push(table.padding[rng.random_range(0..table.padding.len())]);
        }
        out
    }
}
pub struct Decoder {
    profile: Profile,
    index: usize,
    hints: Vec<u8>,
}
impl Decoder {
    pub fn decode(&mut self, input: &[u8]) -> io::Result<Vec<u8>> {
        let mut out = Vec::with_capacity(input.len() / 4);
        for byte in input {
            let table = &self.profile.tables[self.index];
            if table.group_for[*byte as usize].is_none() {
                continue;
            }
            self.hints.push(*byte);
            if self.hints.len() < 4 {
                continue;
            }
            let mut key: [u8; 4] = self.hints[..].try_into().unwrap();
            key.sort_unstable();
            let decoded = table
                .decode
                .get(&key)
                .ok_or_else(|| invalid("invalid Sudoku hint tuple"))?;
            out.push(*decoded);
            self.hints.clear();
            self.index = (self.index + 1) % self.profile.tables.len();
        }
        Ok(out)
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
#[path = "../../tests/finalmask/sudoku.rs"]
mod tests;
