// Hysteria2 protocol constants and helpers — shared.rs

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;

use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

pub const HYSTERIA2_VERSION: u8 = 0x02;

pub const AUTH_OK: u8 = 0x01;
pub const AUTH_ERR: u8 = 0x00;

/// Build an authentication frame to send to the server.
/// Format: [version:1][auth_len:2][auth_payload:auth_len]
pub fn build_auth_frame(hmac: &[u8; 32]) -> Vec<u8> {
    let mut frame = Vec::with_capacity(3 + 32);
    frame.push(HYSTERIA2_VERSION);
    frame.extend_from_slice(&32u16.to_be_bytes());
    frame.extend_from_slice(hmac);
    frame
}

/// Parse an authentication response from the server.
/// Format: [ok:1][version:1] for success, [err:1][msg_len:2][msg] for failure.
pub fn parse_auth_response(data: &[u8]) -> Result<(), Error> {
    if data.is_empty() {
        return Err(Error::Protocol("hysteria2: empty auth response"));
    }
    match data[0] {
        AUTH_OK => Ok(()),
        AUTH_ERR => {
            if data.len() < 3 {
                return Err(Error::Protocol("hysteria2: truncated auth error"));
            }
            Err(Error::Protocol("hysteria2 auth rejected"))
        }
        _ => Err(Error::Protocol("hysteria2: unknown auth response type")),
    }
}

/// Parse a read auth frame from the client.
/// Returns the HMAC bytes.
pub fn parse_auth_frame(data: &[u8]) -> Result<[u8; 32], Error> {
    if data.len() < 3 {
        return Err(Error::Protocol("hysteria2: truncated auth frame"));
    }
    let version = data[0];
    if version != HYSTERIA2_VERSION {
        return Err(Error::Protocol("hysteria2: unsupported version"));
    }
    let auth_len = u16::from_be_bytes([data[1], data[2]]) as usize;
    if auth_len != 32 {
        return Err(Error::Protocol("hysteria2: invalid auth length"));
    }
    if data.len() < 3 + 32 {
        return Err(Error::Protocol("hysteria2: truncated auth payload"));
    }
    let mut hmac = [0u8; 32];
    hmac.copy_from_slice(&data[3..35]);
    Ok(hmac)
}

/// Build the standard Hysteria2 TCPRequest message.
/// Format: [varint 0x401][varint address length][host:port][varint padding length][padding].
pub fn build_tcp_connect_header(address: &Address, port: u16) -> Result<Vec<u8>, Error> {
    let authority = encode_authority(address, port)?;
    let mut header = Vec::with_capacity(16 + authority.len());
    encode_varint(0x401, &mut header)?;
    encode_varint(authority.len() as u64, &mut header)?;
    header.extend_from_slice(authority.as_bytes());
    encode_varint(0, &mut header)?;
    Ok(header)
}

/// Parse a standard Hysteria2 TCPRequest message.
pub fn parse_tcp_connect_header(data: &[u8]) -> Result<(Address, u16), Error> {
    let (request_id, request_id_len) = decode_varint(data)?;
    if request_id != 0x401 {
        return Err(Error::Protocol("hysteria2: expected TCPRequest"));
    }
    let (address_len, address_len_len) = decode_varint(&data[request_id_len..])?;
    let address_start = request_id_len + address_len_len;
    let address_len = usize::try_from(address_len)
        .map_err(|_| Error::Protocol("hysteria2: address length overflow"))?;
    let address_end = address_start
        .checked_add(address_len)
        .ok_or(Error::Protocol("hysteria2: address length overflow"))?;
    if data.len() < address_end {
        return Err(Error::Protocol("hysteria2: truncated TCPRequest address"));
    }
    let authority = core::str::from_utf8(&data[address_start..address_end])
        .map_err(|_| Error::Protocol("hysteria2: invalid TCPRequest address"))?;
    let (padding_len, padding_len_len) = decode_varint(&data[address_end..])?;
    let padding_start = address_end + padding_len_len;
    let padding_len = usize::try_from(padding_len)
        .map_err(|_| Error::Protocol("hysteria2: padding length overflow"))?;
    if data.len() < padding_start.saturating_add(padding_len) {
        return Err(Error::Protocol("hysteria2: truncated TCPRequest padding"));
    }
    parse_authority(authority)
}

/// Build an auth error response.
pub fn build_auth_error(msg: &str) -> Vec<u8> {
    let msg_bytes = msg.as_bytes();
    let mut resp = Vec::with_capacity(3 + msg_bytes.len());
    resp.push(AUTH_ERR);
    resp.extend_from_slice(&(msg_bytes.len() as u16).to_be_bytes());
    resp.extend_from_slice(msg_bytes);
    resp
}

/// Build an auth success response.
pub fn build_auth_ok() -> Vec<u8> {
    vec![AUTH_OK, HYSTERIA2_VERSION]
}

/// Build a TCP connect success response.
pub fn build_connect_ok() -> Vec<u8> {
    vec![0x00, 0x00, 0x00]
}

/// Build a TCP connect error response.
pub fn build_connect_error(msg: &str) -> Vec<u8> {
    let msg_bytes = msg.as_bytes();
    let mut resp = Vec::with_capacity(4 + msg_bytes.len());
    resp.push(0x01);
    let _ = encode_varint(msg_bytes.len() as u64, &mut resp);
    resp.extend_from_slice(msg_bytes);
    let _ = encode_varint(0, &mut resp);
    resp
}

pub(crate) fn encode_varint(value: u64, output: &mut Vec<u8>) -> Result<(), Error> {
    match value {
        0..=63 => output.push(value as u8),
        64..=16_383 => {
            let encoded = (value as u16) | 0x4000;
            output.extend_from_slice(&encoded.to_be_bytes());
        }
        16_384..=1_073_741_823 => {
            let encoded = (value as u32) | 0x8000_0000;
            output.extend_from_slice(&encoded.to_be_bytes());
        }
        1_073_741_824..=4_611_686_018_427_387_903 => {
            let encoded = value | 0xc000_0000_0000_0000;
            output.extend_from_slice(&encoded.to_be_bytes());
        }
        _ => return Err(Error::Protocol("hysteria2: QUIC varint overflow")),
    }
    Ok(())
}

pub(crate) fn decode_varint(data: &[u8]) -> Result<(u64, usize), Error> {
    let first = *data
        .first()
        .ok_or(Error::Protocol("hysteria2: truncated QUIC varint"))?;
    let width = 1usize << (first >> 6);
    if data.len() < width {
        return Err(Error::Protocol("hysteria2: truncated QUIC varint"));
    }
    let mut value = u64::from(first & 0x3f);
    for byte in &data[1..width] {
        value = (value << 8) | u64::from(*byte);
    }
    Ok((value, width))
}

pub(crate) fn encode_authority(address: &Address, port: u16) -> Result<String, Error> {
    match address {
        Address::Ipv4(bytes) => Ok(format!(
            "{}.{}.{}.{}:{port}",
            bytes[0], bytes[1], bytes[2], bytes[3]
        )),
        Address::Ipv6(bytes) => {
            let address = core::net::Ipv6Addr::from(*bytes);
            Ok(format!("[{address}]:{port}"))
        }
        Address::Domain(domain) if !domain.is_empty() => Ok(format!("{domain}:{port}")),
        Address::Domain(_) => Err(Error::Protocol("hysteria2: empty target domain")),
    }
}

pub(crate) fn parse_authority(authority: &str) -> Result<(Address, u16), Error> {
    let (host, port) = if let Some(rest) = authority.strip_prefix('[') {
        let (host, port) = rest
            .split_once("]:")
            .ok_or(Error::Protocol("hysteria2: invalid IPv6 authority"))?;
        (host, port)
    } else {
        authority
            .rsplit_once(':')
            .ok_or(Error::Protocol("hysteria2: target port missing"))?
    };
    if host.is_empty() {
        return Err(Error::Protocol("hysteria2: empty target host"));
    }
    let port = port
        .parse::<u16>()
        .map_err(|_| Error::Protocol("hysteria2: invalid target port"))?;
    let address = if let Ok(ipv4) = host.parse::<core::net::Ipv4Addr>() {
        Address::Ipv4(ipv4.octets())
    } else if let Ok(ipv6) = host.parse::<core::net::Ipv6Addr>() {
        Address::Ipv6(ipv6.octets())
    } else {
        Address::Domain(host.to_string())
    };
    Ok((address, port))
}

// — address encoding helpers —

/// Read exact number of bytes from stream.
pub async fn read_exact<S: AsyncSocket>(stream: &mut S, buf: &mut [u8]) -> Result<(), Error> {
    let mut offset = 0;
    while offset < buf.len() {
        let n = stream
            .read(&mut buf[offset..])
            .await
            .map_err(|_| Error::Io("hysteria2: read failed"))?;
        if n == 0 {
            return Err(Error::Io("hysteria2: unexpected EOF"));
        }
        offset += n;
    }
    Ok(())
}

// ── Crypto helpers (feature-gated, like VLESS reality) ──

#[cfg(feature = "crypto")]
mod crypto {
    use ring::digest;

    /// Derive the HMAC salt from server address + password.
    ///
    /// Used by the outbound (client) side: `SHA256("server:port:password")`.
    /// The inbound side derives the same salt from QUIC keying material instead,
    /// so this function is only needed for outbound connections.
    pub fn derive_salt(server_addr: &str, password: &str) -> [u8; 32] {
        let mut ctx = digest::Context::new(&digest::SHA256);
        ctx.update(server_addr.as_bytes());
        ctx.update(b":");
        ctx.update(password.as_bytes());
        ctx.finish().as_ref().try_into().unwrap()
    }

    /// Compute HMAC-SHA256 over the salt with the password as key.
    pub fn sign_hmac(password: &str, salt: &[u8; 32]) -> [u8; 32] {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, password.as_bytes());
        ring::hmac::sign(&key, salt).as_ref().try_into().unwrap()
    }

    /// Constant-time verification of a client-supplied HMAC.
    ///
    /// Used by the inbound (server) side when the salt has already been
    /// derived from QUIC keying material.
    pub fn verify_hmac(password: &str, salt: &[u8; 32], client_hmac: &[u8; 32]) -> bool {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, password.as_bytes());
        ring::hmac::verify(&key, salt, client_hmac).is_ok()
    }
}

#[cfg(feature = "crypto")]
pub use crypto::*;
