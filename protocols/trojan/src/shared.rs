//! Trojan protocol constants and helpers.

use zero_core::{Address, Error};
use zero_traits::AsyncSocket;

/// Trojan protocol request command types.
pub const CMD_TCP: u8 = 0x01;
pub const CMD_UDP: u8 = 0x03;

/// Address type constants (socks5 compatible).
pub const ATYP_IPV4: u8 = 0x01;
pub const ATYP_DOMAIN: u8 = 0x03;
pub const ATYP_IPV6: u8 = 0x04;

/// CRLF delimiter used in Trojan protocol.
pub const CRLF: &[u8] = b"\r\n";

/// Length of SHA224 hex password in bytes.
pub const PASSWORD_HASH_LEN: usize = 56;

/// Read the password hash + CRLF from a stream.
///
/// Returns the hex password string on success.
pub(crate) async fn read_password<S: AsyncSocket>(stream: &mut S) -> Result<String, Error> {
    let mut buf = [0u8; PASSWORD_HASH_LEN + 2]; // hash + CRLF
    read_exact(stream, &mut buf).await?;
    if &buf[PASSWORD_HASH_LEN..] != CRLF {
        return Err(Error::Protocol("trojan: expected CRLF after password hash"));
    }
    let hex = std::str::from_utf8(&buf[..PASSWORD_HASH_LEN])
        .map_err(|_| Error::Protocol("trojan: invalid password hash"))?;
    Ok(hex.to_owned())
}

/// Read command byte + address + port + CRLF.
pub(crate) async fn read_request<S: AsyncSocket>(
    stream: &mut S,
) -> Result<(u8, Address, u16), Error> {
    let mut head = [0u8; 1]; // cmd
    read_exact(stream, &mut head).await?;
    let cmd = head[0];

    let (addr, port) = read_address(stream).await?;

    let mut crlf = [0u8; 2];
    read_exact(stream, &mut crlf).await?;
    if crlf != CRLF {
        return Err(Error::Protocol("trojan: expected CRLF after address"));
    }

    Ok((cmd, addr, port))
}

/// Write a Trojan UDP packet over a TCP stream.
///
/// Format: `[ATYP][ADDR][PORT][2 bytes payload length BE][CRLF][payload]`.
pub(crate) async fn write_udp_packet<S: AsyncSocket>(
    stream: &mut S,
    addr: &Address,
    port: u16,
    payload: &[u8],
) -> Result<usize, Error> {
    let packet = build_udp_packet(addr, port, payload);
    let len = packet.len();
    stream
        .write_all(&packet)
        .await
        .map_err(|_| Error::Io("trojan: write udp failed"))?;
    Ok(len)
}

/// Build a Trojan UDP packet in memory.
pub(crate) fn build_udp_packet(addr: &Address, port: u16, payload: &[u8]) -> Vec<u8> {
    build_address_body(addr, port, payload)
}

/// Read a Trojan UDP packet from a TCP stream.
///
/// Returns (address, port, payload) on success.
pub(crate) async fn read_udp_packet<S: AsyncSocket>(
    stream: &mut S,
) -> Result<(Address, u16, Vec<u8>), Error> {
    let (target, port) = read_address(stream).await?;

    let mut len_buf = [0u8; 2];
    read_exact(stream, &mut len_buf).await?;
    let payload_len = u16::from_be_bytes(len_buf) as usize;

    let mut crlf = [0u8; 2];
    read_exact(stream, &mut crlf).await?;
    if crlf != CRLF {
        return Err(Error::Protocol("trojan udp: expected CRLF before payload"));
    }

    let mut payload = vec![0u8; payload_len];
    read_exact(stream, &mut payload).await?;

    Ok((target, port, payload))
}

// Internal helpers.

fn build_address_body(addr: &Address, port: u16, payload: &[u8]) -> Vec<u8> {
    let mut body = Vec::new();
    match addr {
        Address::Ipv4(bytes) => {
            body.push(ATYP_IPV4);
            body.extend_from_slice(bytes);
        }
        Address::Ipv6(bytes) => {
            body.push(ATYP_IPV6);
            body.extend_from_slice(bytes);
        }
        Address::Domain(domain) => {
            let b = domain.as_bytes();
            body.push(ATYP_DOMAIN);
            body.push(b.len() as u8);
            body.extend_from_slice(b);
        }
    }
    body.extend_from_slice(&port.to_be_bytes());
    body.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    body.extend_from_slice(CRLF);
    body.extend_from_slice(payload);
    body
}

#[cfg(feature = "crypto")]
pub(crate) fn build_request(
    password: &str,
    addr: &Address,
    port: u16,
    cmd: u8,
) -> Result<Vec<u8>, Error> {
    let mut request = Vec::new();

    use sha2::{Digest, Sha224};
    let digest = Sha224::digest(password.as_bytes());
    request.extend_from_slice(hex::encode(&digest).as_bytes());

    request.extend_from_slice(CRLF);
    request.push(cmd);

    match addr {
        Address::Ipv4(bytes) => {
            request.push(ATYP_IPV4);
            request.extend_from_slice(bytes);
        }
        Address::Ipv6(bytes) => {
            request.push(ATYP_IPV6);
            request.extend_from_slice(bytes);
        }
        Address::Domain(domain) => {
            let bytes = domain.as_bytes();
            if bytes.is_empty() || bytes.len() > 255 {
                return Err(Error::Protocol("trojan: domain too long"));
            }
            request.push(ATYP_DOMAIN);
            request.push(bytes.len() as u8);
            request.extend_from_slice(bytes);
        }
    }

    request.extend_from_slice(&port.to_be_bytes());
    request.extend_from_slice(CRLF);
    Ok(request)
}

#[cfg(not(feature = "crypto"))]
pub(crate) fn build_request(
    _password: &str,
    _addr: &Address,
    _port: u16,
    _cmd: u8,
) -> Result<Vec<u8>, Error> {
    Err(Error::Unsupported("trojan: crypto feature not enabled"))
}

/// Read socks5-style address + port.
async fn read_address<S: AsyncSocket>(stream: &mut S) -> Result<(Address, u16), Error> {
    let mut atyp = [0u8; 1];
    read_exact(stream, &mut atyp).await?;

    let addr = match atyp[0] {
        ATYP_IPV4 => {
            let mut bytes = [0u8; 4];
            read_exact(stream, &mut bytes).await?;
            Address::Ipv4(bytes)
        }
        ATYP_IPV6 => {
            let mut bytes = [0u8; 16];
            read_exact(stream, &mut bytes).await?;
            Address::Ipv6(bytes)
        }
        ATYP_DOMAIN => {
            let mut len = [0u8; 1];
            read_exact(stream, &mut len).await?;
            let mut domain = vec![0u8; len[0] as usize];
            read_exact(stream, &mut domain).await?;
            let domain = String::from_utf8(domain)
                .map_err(|_| Error::Protocol("trojan: invalid domain encoding"))?;
            Address::Domain(domain)
        }
        _ => return Err(Error::Protocol("trojan: unsupported address type")),
    };

    let mut port_bytes = [0u8; 2];
    read_exact(stream, &mut port_bytes).await?;
    let port = u16::from_be_bytes(port_bytes);

    Ok((addr, port))
}

async fn read_exact<S: AsyncSocket>(stream: &mut S, buf: &mut [u8]) -> Result<(), Error> {
    let mut offset = 0;
    while offset < buf.len() {
        let n = stream
            .read(&mut buf[offset..])
            .await
            .map_err(|_| Error::Protocol("trojan: read failed"))?;
        if n == 0 {
            return Err(Error::Protocol("trojan: unexpected EOF"));
        }
        offset += n;
    }
    Ok(())
}

#[cfg(feature = "crypto")]
pub mod hex {
    pub fn encode(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
