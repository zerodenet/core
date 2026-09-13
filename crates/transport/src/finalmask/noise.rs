//! Per-peer first-packet noise and timed reset, as a carrier write prelude.
use super::tcp::Range;
use rand::Rng;
use std::{collections::HashMap, io, net::SocketAddr, time::Duration};
#[derive(Debug, Clone, Default)]
pub struct Settings {
    pub reset_seconds: Range,
    pub items: Vec<Item>,
}
#[derive(Debug, Clone)]
pub struct Item {
    pub packet: Vec<u8>,
    pub random_length: Range,
    pub minimum_byte: u8,
    pub maximum_byte: u8,
    pub delay_ms: Range,
}
pub(super) struct State {
    settings: Settings,
    peers: HashMap<SocketAddr, Option<tokio::time::Instant>>,
    pending: Option<SocketAddr>,
    sweep: tokio::time::Instant,
}
pub struct Emission {
    pub packet: Vec<u8>,
    pub delay: Duration,
    pub payload: bool,
}
impl State {
    pub(super) fn new(settings: &Settings) -> io::Result<Self> {
        settings.reset_seconds.validate()?;
        let mut size = 0u64;
        if settings.items.len() > 256
            || settings.reset_seconds.maximum > i64::MAX as u64 / 1_000_000_000
        {
            return Err(invalid(
                "noise sequence or reset duration exceeds its bound",
            ));
        }
        for item in &settings.items {
            item.random_length.validate()?;
            item.delay_ms.validate()?;
            if item.minimum_byte > item.maximum_byte
                || (!item.packet.is_empty() && item.random_length.maximum > 0)
                || item.delay_ms.maximum > i64::MAX as u64 / 1_000_000
            {
                return Err(invalid("invalid noise packet, delay or random byte range"));
            }
            size = size
                .checked_add(item.random_length.maximum.max(item.packet.len() as u64))
                .ok_or_else(|| invalid("noise sequence too large"))?;
        }
        if size > 65536 {
            return Err(invalid("noise sequence exceeds 64 KiB"));
        }
        Ok(Self {
            settings: settings.clone(),
            peers: HashMap::new(),
            pending: None,
            sweep: tokio::time::Instant::now(),
        })
    }
    pub(super) fn before(&mut self, peer: SocketAddr) -> io::Result<Vec<Emission>> {
        let now = tokio::time::Instant::now();
        if now >= self.sweep {
            self.peers
                .retain(|_, deadline| deadline.is_none_or(|deadline| now <= deadline));
            self.sweep = now + Duration::from_secs(1);
        }
        if self
            .peers
            .get(&peer)
            .is_some_and(|deadline| deadline.is_none_or(|deadline| now <= deadline))
        {
            return Ok(Vec::new());
        }
        if self.peers.len() >= 4096 && !self.peers.contains_key(&peer) {
            return Err(invalid("noise peer capacity reached"));
        }
        let mut emissions = Vec::new();
        for item in &self.settings.items {
            let packet = if item.random_length.maximum == 0 {
                item.packet.clone()
            } else {
                let length = item.random_length.sample() as usize;
                let mut rng = rand::rng();
                (0..length)
                    .map(|_| rng.random_range(item.minimum_byte..=item.maximum_byte))
                    .collect()
            };
            emissions.push(Emission {
                packet,
                delay: Duration::from_millis(item.delay_ms.sample()),
                payload: false,
            });
        }
        self.peers.insert(peer, None);
        self.pending = Some(peer);
        Ok(emissions)
    }
    pub(super) fn finish(&mut self, peer: SocketAddr) {
        if self.pending == Some(peer) {
            self.pending = None;
            if self.settings.reset_seconds.maximum != 0 {
                self.peers.insert(
                    peer,
                    Some(
                        tokio::time::Instant::now()
                            + Duration::from_secs(self.settings.reset_seconds.sample()),
                    ),
                );
            }
        }
    }
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
#[path = "../../tests/finalmask/noise.rs"]
mod tests;
