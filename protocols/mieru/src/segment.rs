// Mieru protocol segment framing — segment.rs
//
// Segment wire format:
//   [padding 0]  (variable, optional)
//   [nonce]      (0 or 24 bytes)
//   [encrypted metadata (32) + auth tag (16)]
//   [padding 1]  (variable, length = prefix_length from data metadata)
//   [encrypted payload + auth tag (16)]  (optional, when payload_length > 0)
//   [padding 2]  (variable, length = suffix_length)

use alloc::vec;
use alloc::vec::Vec;

use zero_core::Error;

use crate::crypto::MieruCipher;
use crate::metadata::{
    DataMetadata, SessionMetadata, ACK_CLIENT_TO_SERVER, ACK_SERVER_TO_CLIENT,
    CLOSE_SESSION_REQUEST, CLOSE_SESSION_RESPONSE, DATA_CLIENT_TO_SERVER, DATA_SERVER_TO_CLIENT,
    METADATA_LEN, OPEN_SESSION_REQUEST, OPEN_SESSION_RESPONSE,
};

/// Maximum TCP fragment size.
pub const MAX_FRAGMENT: usize = 32768;

/// Auth tag length for XChaCha20-Poly1305.
const TAG_LEN: usize = 16;

/// A parsed Mieru segment.
#[derive(Debug, Clone)]
pub struct Segment {
    /// Session-level metadata (for open/close).
    pub session_meta: Option<SessionMetadata>,
    /// Data-level metadata (for data/ACK).
    pub data_meta: Option<DataMetadata>,
    /// Decrypted payload.
    pub payload: Vec<u8>,
}

// ── Build ────────────────────────────────────────────────────────────

/// Build a session control segment (open/close).
pub fn build_session_segment(
    meta: &SessionMetadata,
    payload: &[u8],
    cipher: &mut MieruCipher,
    include_nonce: bool,
) -> Result<Vec<u8>, Error> {
    let padding = random_bytes(meta.suffix_length as usize);
    build_session_segment_with_padding(meta, payload, cipher, include_nonce, &padding)
}

/// Builds a session control segment with caller-selected suffix padding.
/// Traffic-pattern padding must use this entry point so its byte shape, not
/// merely its length, reaches the wire.
pub fn build_session_segment_with_padding(
    meta: &SessionMetadata,
    payload: &[u8],
    cipher: &mut MieruCipher,
    include_nonce: bool,
    padding: &[u8],
) -> Result<Vec<u8>, Error> {
    if payload.len() != meta.payload_length as usize {
        return Err(Error::Protocol("mieru: payload length mismatch"));
    }
    if padding.len() != meta.suffix_length as usize {
        return Err(Error::Protocol("mieru: suffix padding length mismatch"));
    }
    let meta_bytes = meta.encode();

    // Set nonce inclusion for this segment
    cipher.set_include_nonce(include_nonce);
    let encrypted_meta = cipher.encrypt(&meta_bytes)?;

    let encrypted_payload = if !payload.is_empty() {
        cipher.encrypt(payload)?
    } else {
        Vec::new()
    };

    // Session segment wire format matches upstream mieru (underlay_stream.go):
    //   [nonce(24) + encryptedMeta(48)] + [encryptedPayload] + [suffixPadding]
    // There is NO leading padding0 — the server's readOneSegment() reads the
    // first 72 bytes directly as nonce + encrypted metadata. Any prefix bytes
    // would be misread as the nonce and break AEAD decryption.
    // Suffix padding (declared in metadata.suffix_length) is appended after the
    // payload; with suffix_length = 0 (the default), none is added.
    let mut buf = Vec::new();
    buf.extend_from_slice(&encrypted_meta);

    if !encrypted_payload.is_empty() {
        buf.extend_from_slice(&encrypted_payload);
    }

    buf.extend_from_slice(padding);
    Ok(buf)
}

/// Build a data transfer segment.
pub fn build_data_segment(
    meta: &DataMetadata,
    payload: &[u8],
    cipher: &mut MieruCipher,
    include_nonce: bool,
) -> Result<Vec<u8>, Error> {
    if payload.len() != meta.payload_length as usize {
        return Err(Error::Protocol("mieru: payload length mismatch"));
    }
    let meta_bytes = meta.encode();

    cipher.set_include_nonce(include_nonce);
    let encrypted_meta = cipher.encrypt(&meta_bytes)?;

    let encrypted_payload = if !payload.is_empty() {
        cipher.encrypt(payload)?
    } else {
        Vec::new()
    };

    let mut buf = Vec::new();
    // No padding0 for data/ACK segments — only session segments
    // (open/close) carry padding0 per upstream mieru protocol.

    buf.extend_from_slice(&encrypted_meta);

    // padding 1 (prefix_length random bytes)
    if meta.prefix_length > 0 {
        let pad1 = random_bytes(meta.prefix_length as usize);
        buf.extend_from_slice(&pad1);
    }

    if !encrypted_payload.is_empty() {
        buf.extend_from_slice(&encrypted_payload);
    }

    // padding 2 (suffix_length random bytes)
    if meta.suffix_length > 0 {
        let pad2 = random_bytes(meta.suffix_length as usize);
        buf.extend_from_slice(&pad2);
    }

    Ok(buf)
}

/// Build an ACK segment (no payload).
pub fn build_ack_segment(
    meta: &DataMetadata,
    cipher: &mut MieruCipher,
    include_nonce: bool,
) -> Result<Vec<u8>, Error> {
    build_data_segment(meta, &[], cipher, include_nonce)
}

// ── Parse ────────────────────────────────────────────────────────────

/// Parse a raw segment from the wire.
///
/// Returns the parsed segment and the number of bytes consumed.
pub fn parse_segment(
    data: &[u8],
    cipher: &mut MieruCipher,
    has_nonce: bool,
    _is_session: bool,
) -> Result<(Segment, usize), Error> {
    // An incomplete frame must not advance the connection's implicit nonce.
    let mut candidate = cipher.clone();
    let result = parse_complete_segment(data, &mut candidate, has_nonce)?;
    *cipher = candidate;
    Ok(result)
}

fn parse_complete_segment(
    data: &[u8],
    cipher: &mut MieruCipher,
    has_nonce: bool,
) -> Result<(Segment, usize), Error> {
    // Decrypt metadata (with optional padding0 scanning on first segment).
    let (meta_bytes, mut offset) = if has_nonce {
        // Try offsets 0..PADDING0_MAX to account for optional padding0
        let mut best_offset = 0usize;
        let mut best_meta = None;
        for try_off in 0..=PADDING0_MAX {
            let start = try_off;
            let end = start + 24 + METADATA_LEN + TAG_LEN;
            if data.len() < end {
                if best_meta.is_some() {
                    break;
                }
                continue;
            }
            let chunk = &data[start..end];
            // Try decrypting; if valid, use this offset
            if let Ok(pt) = cipher.decrypt(true, chunk) {
                if pt.len() >= METADATA_LEN {
                    let ptype = pt[0];
                    if (2..=9).contains(&ptype) {
                        best_offset = end;
                        best_meta = Some(pt);
                        break;
                    }
                }
            }
        }
        match best_meta {
            Some(pt) => (pt, best_offset),
            None => {
                // Fallback: try offset 0 without scanning
                if data.len() < 24 + METADATA_LEN + TAG_LEN {
                    return Err(Error::Protocol("mieru: need more data"));
                }
                let chunk = &data[..24 + METADATA_LEN + TAG_LEN];
                (cipher.decrypt(true, chunk)?, 24 + METADATA_LEN + TAG_LEN)
            }
        }
    } else {
        if data.len() < METADATA_LEN + TAG_LEN {
            return Err(Error::Protocol("mieru: need more data"));
        }
        let chunk = &data[..METADATA_LEN + TAG_LEN];
        (cipher.decrypt(false, chunk)?, METADATA_LEN + TAG_LEN)
    };

    let protocol_type = meta_bytes[0];

    match protocol_type {
        OPEN_SESSION_REQUEST
        | OPEN_SESSION_RESPONSE
        | CLOSE_SESSION_REQUEST
        | CLOSE_SESSION_RESPONSE => {
            let session_meta = SessionMetadata::decode(&meta_bytes);

            // Decrypt payload if any (implicit nonce — cipher tracks it)
            let mut payload = Vec::new();
            if session_meta.payload_length > 0 {
                let payload_len = session_meta.payload_length as usize;
                if data.len() < offset + payload_len + TAG_LEN {
                    return Err(Error::Protocol("mieru: need more data"));
                }
                let pdata = data[offset..offset + payload_len + TAG_LEN].to_vec();
                payload = cipher.decrypt(false, &pdata)?;
                offset += payload_len + TAG_LEN;
            }

            offset += session_meta.suffix_length as usize;
            if data.len() < offset {
                return Err(Error::Protocol("mieru: need more data"));
            }
            Ok((
                Segment {
                    session_meta: Some(session_meta),
                    data_meta: None,
                    payload,
                },
                offset,
            ))
        }
        DATA_CLIENT_TO_SERVER
        | DATA_SERVER_TO_CLIENT
        | ACK_CLIENT_TO_SERVER
        | ACK_SERVER_TO_CLIENT => {
            let data_meta = DataMetadata::decode(&meta_bytes);

            // Skip padding 1
            offset += data_meta.prefix_length as usize;

            // Decrypt payload if any (implicit nonce — cipher tracks it)
            let mut payload = Vec::new();
            if data_meta.payload_length > 0 {
                let payload_len = data_meta.payload_length as usize;
                if data.len() < offset + payload_len + TAG_LEN {
                    return Err(Error::Protocol("mieru: need more data"));
                }
                let pdata = data[offset..offset + payload_len + TAG_LEN].to_vec();
                payload = cipher.decrypt(false, &pdata)?;
                offset += payload_len + TAG_LEN;
            }

            // Skip padding 2
            offset += data_meta.suffix_length as usize;
            if data.len() < offset {
                return Err(Error::Protocol("mieru: need more data"));
            }

            Ok((
                Segment {
                    session_meta: None,
                    data_meta: Some(data_meta),
                    payload,
                },
                offset,
            ))
        }
        _ => Err(Error::Protocol("mieru: unknown protocol type")),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────

#[cfg(not(feature = "crypto"))]
fn random_bytes(_len: usize) -> Vec<u8> {
    Vec::new()
}

#[cfg(feature = "crypto")]
fn random_bytes(len: usize) -> Vec<u8> {
    use rand::RngCore;
    let mut buf = vec![0u8; len];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    buf
}

/// Maximum leading padding0 offset scanned by `parse_segment` on the first
/// (nonce-carrying) segment. Upstream mieru does not emit a leading padding0,
/// so the nonce is normally at offset 0; the scan is a permissive fallback.
const PADDING0_MAX: usize = 64;
