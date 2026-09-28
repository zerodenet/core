//! Bounded, protocol-neutral Packet address translation for ICMP Echo.
//!
//! This is a Packet adapter, not a third L4 flow type. The caller owns I/O
//! and supplies an opaque return context; this module owns correlation.

use std::{
    collections::HashMap,
    io,
    net::IpAddr,
    time::{Duration, Instant},
};

use crate::packet;

const MAX_REQUESTS: usize = 1_024;
const MAX_BYTES: usize = 2 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

type Key = (IpAddr, IpAddr, u16);

pub struct EchoTranslation<C> {
    pending: HashMap<Key, Pending<C>>,
    next_id: u16,
    bytes: usize,
}

struct Pending<C> {
    original: Vec<u8>,
    translated: Vec<u8>,
    context: C,
    expires: Instant,
}

impl<C> Default for EchoTranslation<C> {
    fn default() -> Self {
        Self {
            pending: HashMap::new(),
            next_id: rand::random(),
            bytes: 0,
        }
    }
}

impl<C> EchoTranslation<C> {
    pub fn request(
        &mut self,
        packet: &[u8],
        local: IpAddr,
        context: C,
        now: Instant,
    ) -> io::Result<Vec<u8>> {
        self.expire(now, |_| false);
        let request = packet::parse_icmp_echo_request(packet).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Unsupported,
                "translation supports ICMP Echo requests only",
            )
        })?;
        if self.pending.len() >= MAX_REQUESTS
            || self.bytes.saturating_add(packet.len().saturating_mul(2)) > MAX_BYTES
        {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "ICMP translation limit",
            ));
        }
        let remote = request.destination;
        let key = loop {
            self.next_id = self.next_id.wrapping_add(1);
            let key = (local, remote, self.next_id);
            if !self.pending.contains_key(&key) {
                break key;
            }
        };
        let translated = packet::translate_echo_request(packet, local, key.2).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid ICMP address translation",
            )
        })?;
        self.bytes += packet.len() + translated.len();
        self.pending.insert(
            key,
            Pending {
                original: packet.to_vec(),
                translated: translated.clone(),
                context,
                expires: now + REQUEST_TIMEOUT,
            },
        );
        Ok(translated)
    }

    pub fn response(&mut self, packet: &[u8], now: Instant) -> Option<(C, Vec<u8>)> {
        if self.pending.is_empty() {
            return None;
        }
        let key = packet::echo_response_key(packet)?;
        self.expire(now, |_| false);
        let pending = self.pending.get(&key)?;
        let restored =
            packet::restore_echo_response(packet, &pending.original, &pending.translated)?;
        let pending = self.pending.remove(&key)?;
        self.bytes -= pending.original.len() + pending.translated.len();
        Some((pending.context, restored))
    }

    pub fn expire(&mut self, now: Instant, closed: impl Fn(&C) -> bool) {
        self.pending.retain(|_, pending| {
            if now >= pending.expires || closed(&pending.context) {
                self.bytes -= pending.original.len() + pending.translated.len();
                false
            } else {
                true
            }
        });
    }

    pub fn clear(&mut self) {
        self.pending.clear();
        self.bytes = 0;
    }
}
