use crate::traffic_pattern::{data_padding_lengths, session_padding, TrafficPattern};
use crate::{crypto::MieruCipher, metadata::*, segment::Segment, session::MieruSession};
use rand::RngCore;
use std::{
    io,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

#[derive(Clone)]
pub(crate) struct PacketCodec {
    key: [u8; 32],
    username: String,
    pattern: TrafficPattern,
    pattern_applied: Arc<AtomicBool>,
    payload_limit: usize,
    fragment_size: NonZeroUsize,
}
impl PacketCodec {
    #[cfg(test)]
    pub(crate) fn new(key: [u8; 32], username: &str) -> Self {
        Self::configured(key, username, &Default::default(), false)
            .expect("default Mieru options are valid")
    }
    pub(crate) fn configured(
        key: [u8; 32],
        username: &str,
        options: &mieru_config::MieruTransportOptions,
        ipv6: bool,
    ) -> io::Result<Self> {
        options.validate().map_err(io::Error::other)?;
        Ok(Self {
            key,
            username: username.into(),
            pattern: TrafficPattern::from_config(options.traffic_pattern.as_ref())
                .map_err(io::Error::other)?,
            pattern_applied: Arc::new(AtomicBool::new(false)),
            payload_limit: options.udp_payload_limit(ipv6),
            fragment_size: NonZeroUsize::new(options.fragment_size(ipv6))
                .ok_or_else(|| io::Error::other("mieru MTU leaves no payload space"))?,
        })
    }
    pub(crate) fn fragment_size(&self) -> NonZeroUsize {
        self.fragment_size
    }
    pub(crate) fn accept_open(&self, wire: &[u8]) -> bool {
        wire.len() >= 24 && super::replay::fresh_open(self.key, wire)
    }
    pub(crate) fn encode(&self, segment: &Segment) -> io::Result<Vec<u8>> {
        // Padding and fragmentation both consume the same outer-MTU budget.
        let base = 72 + segment.payload.len() + if segment.payload.is_empty() { 0 } else { 16 };
        let budget = self
            .payload_limit
            .checked_sub(base)
            .ok_or_else(|| io::Error::other("mieru packet exceeds MTU"))?;
        let mut prefix = Vec::new();
        let suffix;
        let meta = if let Some(meta) = &segment.session_meta {
            let mut meta = meta.clone();
            suffix = session_padding(budget, 88 + segment.payload.len(), &self.username);
            meta.suffix_length = suffix.len() as u8;
            meta.encode()
        } else {
            let mut meta = segment
                .data_meta
                .as_ref()
                .ok_or_else(|| io::Error::other("missing packet metadata"))?
                .clone();
            let (before, after) = if meta.prefix_length != 0 || meta.suffix_length != 0 {
                if usize::from(meta.prefix_length) + usize::from(meta.suffix_length) > budget {
                    return Err(io::Error::other("mieru packet padding exceeds MTU"));
                }
                (meta.prefix_length, meta.suffix_length)
            } else {
                data_padding_lengths(budget)
            };
            prefix.resize(before as usize, 0);
            let mut after_bytes = vec![0; after as usize];
            rand::rngs::OsRng.fill_bytes(&mut prefix);
            rand::rngs::OsRng.fill_bytes(&mut after_bytes);
            suffix = after_bytes;
            meta.prefix_length = before;
            meta.suffix_length = after;
            meta.encode()
        };
        let first = !self.pattern_applied.swap(true, Ordering::Relaxed);
        let mut cipher = MieruCipher::with_config(
            &self.key,
            &self.pattern.nonce_config(
                &self.username,
                first || self.pattern.nonce.apply_to_all_udp_packet,
            ),
        );
        let nonce = *cipher.current_nonce();
        let mut wire = cipher.encrypt(&meta).map_err(io::Error::other)?;
        // The reference stateless carrier uses the same explicit packet nonce
        // for the separately encrypted metadata and payload.
        wire.extend(prefix);
        if !segment.payload.is_empty() {
            let mut payload_cipher = MieruCipher::with_nonce(&self.key, nonce);
            payload_cipher.set_include_nonce(false);
            wire.extend(
                payload_cipher
                    .encrypt(&segment.payload)
                    .map_err(io::Error::other)?,
            );
        }
        wire.extend(suffix);
        if wire.len() > self.payload_limit {
            return Err(io::Error::other("mieru packet exceeds MTU"));
        }
        Ok(wire)
    }
    pub(crate) fn decode(&self, wire: &[u8]) -> io::Result<Segment> {
        if !(72..=1500).contains(&wire.len()) {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let nonce: [u8; 24] = wire[..24].try_into().unwrap();
        let mut cipher = MieruCipher::with_nonce(&self.key, nonce);
        let plain = cipher
            .decrypt(true, &wire[..72])
            .map_err(io::Error::other)?;
        let (session_meta, data_meta, prefix, length, suffix, timestamp) = match plain[0] {
            OPEN_SESSION_REQUEST
            | OPEN_SESSION_RESPONSE
            | CLOSE_SESSION_REQUEST
            | CLOSE_SESSION_RESPONSE => {
                let m = SessionMetadata::decode(&plain);
                if matches!(
                    m.protocol_type,
                    OPEN_SESSION_REQUEST | OPEN_SESSION_RESPONSE
                ) && m.payload_length > 1024
                {
                    return Err(io::ErrorKind::InvalidData.into());
                }
                (
                    Some(m.clone()),
                    None,
                    0,
                    m.payload_length,
                    m.suffix_length,
                    m.timestamp,
                )
            }
            DATA_CLIENT_TO_SERVER
            | DATA_SERVER_TO_CLIENT
            | ACK_CLIENT_TO_SERVER
            | ACK_SERVER_TO_CLIENT => {
                let m = DataMetadata::decode(&plain);
                (
                    None,
                    Some(m.clone()),
                    m.prefix_length,
                    m.payload_length,
                    m.suffix_length,
                    m.timestamp,
                )
            }
            _ => return Err(io::ErrorKind::InvalidData.into()),
        };
        if MieruSession::timestamp_minutes().abs_diff(timestamp) > 3 {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let start = 72 + prefix as usize;
        let end = start + length as usize + if length > 0 { 16 } else { 0 };
        if end + suffix as usize != wire.len() {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let payload = if length > 0 {
            MieruCipher::with_nonce(&self.key, nonce)
                .decrypt(false, &wire[start..end])
                .map_err(io::Error::other)?
        } else {
            Vec::new()
        };
        Ok(Segment {
            session_meta,
            data_meta,
            payload,
        })
    }
}
