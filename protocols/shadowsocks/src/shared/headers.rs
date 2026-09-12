use super::*;
// 2022 edition TCP header (SIP022 section 3.1.3).
//
// Request stream:  salt | fixed-header-chunk(nonce 0) | var-header-chunk(nonce 1)
//                          | [ length-chunk | payload-chunk ]*
// Response stream: salt | fixed-header-chunk(nonce 0, acts as first length chunk)
//                          | payload-chunk(nonce 1) | [ length-chunk | payload-chunk ]*
//
// The fixed/var headers and the response header are each a single AEAD
// operation (one nonce increment), unlike the legacy length+payload pair
// (two operations) used for body chunks.

pub const SS_2022_HEADER_TYPE_CLIENT_STREAM: u8 = 0;
pub const SS_2022_HEADER_TYPE_SERVER_STREAM: u8 = 1;
/// SIP022 3.2.3 UDP main-header socket types (same numeric values as streams).
pub const SS_2022_HEADER_TYPE_CLIENT_PACKET: u8 = 0;
pub const SS_2022_HEADER_TYPE_SERVER_PACKET: u8 = 1;
pub const SS_2022_MAX_PADDING_LENGTH: usize = 900;
/// Messages with over this many seconds of time skew are treated as replay.
pub const SS_2022_TIMESTAMP_WINDOW_SECS: u64 = 30;
/// Fixed request header is always 11 bytes: type(1) + timestamp(8) + length(2).
pub const SS_2022_REQUEST_FIXED_HEADER_LEN: usize = 11;

/// Build the request fixed-length header plaintext (11 bytes).
pub fn build_2022_request_fixed_header(
    timestamp: u64,
    var_header_len: u16,
) -> [u8; SS_2022_REQUEST_FIXED_HEADER_LEN] {
    let mut buf = [0u8; SS_2022_REQUEST_FIXED_HEADER_LEN];
    buf[0] = SS_2022_HEADER_TYPE_CLIENT_STREAM;
    buf[1..9].copy_from_slice(&timestamp.to_be_bytes());
    buf[9..11].copy_from_slice(&var_header_len.to_be_bytes());
    buf
}

/// Parse the request fixed-length header plaintext.
/// Returns `(type, timestamp, var_header_len)`.
pub fn parse_2022_request_fixed_header(plain: &[u8]) -> Result<(u8, u64, u16), Error> {
    if plain.len() != SS_2022_REQUEST_FIXED_HEADER_LEN {
        return Err(Error::Protocol("ss: 2022 request fixed header bad length"));
    }
    let mut ts = [0u8; 8];
    ts.copy_from_slice(&plain[1..9]);
    let timestamp = u64::from_be_bytes(ts);
    let var_len = u16::from_be_bytes([plain[9], plain[10]]);
    Ok((plain[0], timestamp, var_len))
}

/// Build the request variable-length header plaintext.
///
/// Layout: `ATYP + addr + port(2 BE) + padding_length(2 BE) + padding + initial_payload`.
pub fn build_2022_request_var_header(
    addr: &Address,
    port: u16,
    padding: &[u8],
    initial_payload: &[u8],
) -> Result<Vec<u8>, Error> {
    if padding.len() > SS_2022_MAX_PADDING_LENGTH {
        return Err(Error::Protocol("ss: 2022 padding exceeds max"));
    }
    let addr_bytes = encode_address(addr)?;
    let mut buf =
        Vec::with_capacity(addr_bytes.len() + 2 + 2 + padding.len() + initial_payload.len());
    buf.extend_from_slice(&addr_bytes);
    buf.extend_from_slice(&port.to_be_bytes());
    buf.extend_from_slice(&(padding.len() as u16).to_be_bytes());
    buf.extend_from_slice(padding);
    buf.extend_from_slice(initial_payload);
    Ok(buf)
}

/// Parse the request variable-length header plaintext.
///
/// Returns `(target, port, initial_payload)`. Padding is validated and discarded.
/// Rejects headers with neither initial payload nor padding per SIP022 3.1.3.
pub fn parse_2022_request_var_header(plain: &[u8]) -> Result<(Address, u16, Vec<u8>), Error> {
    let (addr, addr_end) = decode_address(plain)?;
    if plain.len() < addr_end + 2 {
        return Err(Error::Protocol("ss: 2022 var header truncated port"));
    }
    let port = u16::from_be_bytes([plain[addr_end], plain[addr_end + 1]]);
    let mut cursor = addr_end + 2;

    if plain.len() < cursor + 2 {
        return Err(Error::Protocol(
            "ss: 2022 var header truncated padding length",
        ));
    }
    let padding_len = u16::from_be_bytes([plain[cursor], plain[cursor + 1]]) as usize;
    if padding_len > SS_2022_MAX_PADDING_LENGTH {
        return Err(Error::Protocol("ss: 2022 padding exceeds max"));
    }
    cursor += 2;
    if plain.len() < cursor + padding_len {
        return Err(Error::Protocol("ss: 2022 var header truncated padding"));
    }
    cursor += padding_len;

    let initial_payload = plain[cursor..].to_vec();
    // SIP022 3.1.3: reject if neither payload nor padding is present.
    if initial_payload.is_empty() && padding_len == 0 {
        return Err(Error::Protocol(
            "ss: 2022 var header needs payload or padding",
        ));
    }
    Ok((addr, port, initial_payload))
}

/// Build the response fixed-length header plaintext.
///
/// Layout: `type(1) + timestamp(8 BE) + request_salt(16/32) + length(2 BE)`.
/// `length` is the plaintext length of the first payload chunk that follows.
pub fn build_2022_response_fixed_header(
    timestamp: u64,
    request_salt: &[u8],
    length: u16,
) -> Result<Vec<u8>, Error> {
    let mut buf = Vec::with_capacity(1 + 8 + request_salt.len() + 2);
    buf.push(SS_2022_HEADER_TYPE_SERVER_STREAM);
    buf.extend_from_slice(&timestamp.to_be_bytes());
    buf.extend_from_slice(request_salt);
    buf.extend_from_slice(&length.to_be_bytes());
    Ok(buf)
}

/// Plaintext length of the response fixed-length header for a given key size.
pub const fn ss_2022_response_header_plain_len(salt_len: usize) -> usize {
    1 + 8 + salt_len + 2
}

/// Parse the response fixed-length header plaintext.
///
/// Returns `(type, timestamp, request_salt, length)`. Validates the type byte.
pub fn parse_2022_response_fixed_header(
    plain: &[u8],
    salt_len: usize,
) -> Result<(u8, u64, Vec<u8>, u16), Error> {
    let need = ss_2022_response_header_plain_len(salt_len);
    if plain.len() != need {
        return Err(Error::Protocol("ss: 2022 response header bad length"));
    }
    if plain[0] != SS_2022_HEADER_TYPE_SERVER_STREAM {
        return Err(Error::Protocol("ss: 2022 response header bad type"));
    }
    let mut ts = [0u8; 8];
    ts.copy_from_slice(&plain[1..9]);
    let timestamp = u64::from_be_bytes(ts);
    let request_salt = plain[9..9 + salt_len].to_vec();
    let length = u16::from_be_bytes([plain[9 + salt_len], plain[9 + salt_len + 1]]);
    Ok((plain[0], timestamp, request_salt, length))
}

/// Encrypt a single 2022 AEAD chunk (one nonce increment).
///
/// Unlike [`encrypt_tcp_chunk`] which emits a length+payload pair (two
/// operations), this performs exactly one AEAD seal and advances the nonce
/// counter by one. Used for the 2022 request/response header chunks.
/// Generate random-length padding for a 2022 request header.
///
/// Returns padding of random length in `[0, SS_2022_MAX_PADDING_LENGTH]`. When
/// `ensure_nonzero` is true (no initial payload available), the length is
/// forced to at least 1, as required by SIP022 3.1.3.
#[cfg(all(feature = "crypto", feature = "blake3"))]
pub fn random_2022_padding(ensure_nonzero: bool) -> Result<Vec<u8>, Error> {
    let mut len_bytes = [0u8; 2];
    fill_random(&mut len_bytes)?;
    let mut len = (u16::from_be_bytes(len_bytes) as usize) % (SS_2022_MAX_PADDING_LENGTH + 1);
    if ensure_nonzero && len == 0 {
        len = 1;
    }
    let mut padding = vec![0u8; len];
    fill_random(&mut padding)?;
    Ok(padding)
}

/// Validate a 2022 header timestamp against the current system time.
///
/// SIP022 3.1.5: messages with over 30 seconds of skew are treated as replay.
#[cfg(all(feature = "crypto", feature = "blake3"))]
pub fn validate_2022_timestamp(timestamp: u64) -> Result<(), Error> {
    let now = now_unix_seconds();
    let diff = now
        .saturating_sub(timestamp)
        .max(timestamp.saturating_sub(now));
    if diff > SS_2022_TIMESTAMP_WINDOW_SECS {
        return Err(Error::Protocol("ss: 2022 timestamp outside replay window"));
    }
    Ok(())
}
