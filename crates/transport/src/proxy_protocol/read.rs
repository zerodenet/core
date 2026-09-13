use std::{
    io,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};
use tokio::io::{AsyncRead, AsyncReadExt};
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid PROXY protocol preamble",
    )
}

/// Enabled listeners require a preamble. Read exactly its bytes so a coalesced
/// TLS ClientHello/HTTP request remains on the carrier for the next layer.
pub async fn accept<S: AsyncRead + Unpin>(
    stream: &mut S,
) -> io::Result<Option<(SocketAddr, SocketAddr)>> {
    let mut first = [0; 6];
    stream.read_exact(&mut first).await?;
    if &first == b"PROXY " {
        let mut line = Vec::from(first);
        while !line.ends_with(b"\r\n") {
            if line.len() >= 107 {
                return Err(invalid());
            }
            line.push(stream.read_u8().await?);
        }
        return version_one(&line);
    }
    if first != *b"\r\n\r\n\0\r" {
        return Err(invalid());
    }
    let mut rest = [0; 10];
    stream.read_exact(&mut rest).await?;
    if rest[..6] != *b"\nQUIT\n" || rest[6] >> 4 != 2 {
        return Err(invalid());
    }
    let mut payload = vec![0; usize::from(u16::from_be_bytes([rest[8], rest[9]]))];
    stream.read_exact(&mut payload).await?;
    match rest[6] & 15 {
        0 => Ok(None),
        1 => version_two(rest[7], &payload),
        _ => Err(invalid()),
    }
}
fn version_one(line: &[u8]) -> io::Result<Option<(SocketAddr, SocketAddr)>> {
    let line = std::str::from_utf8(line).map_err(|_| invalid())?;
    let parts: Vec<_> = line.trim_end_matches("\r\n").split(' ').collect();
    if parts.get(1) == Some(&"UNKNOWN") {
        return Ok(None);
    }
    if parts.len() != 6 {
        return Err(invalid());
    }
    let source: IpAddr = parts[2].parse().map_err(|_| invalid())?;
    let destination: IpAddr = parts[3].parse().map_err(|_| invalid())?;
    if !matches!(
        (parts[1], source, destination),
        ("TCP4", IpAddr::V4(_), IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_), IpAddr::V6(_))
    ) {
        return Err(invalid());
    }
    Ok(Some((
        SocketAddr::new(source, parts[4].parse().map_err(|_| invalid())?),
        SocketAddr::new(destination, parts[5].parse().map_err(|_| invalid())?),
    )))
}
fn version_two(family: u8, payload: &[u8]) -> io::Result<Option<(SocketAddr, SocketAddr)>> {
    let (source, destination, offset) = match family {
        0 => return Ok(None),
        0x11 | 0x12 if payload.len() >= 12 => (
            IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(&payload[..4]).unwrap())),
            IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(&payload[4..8]).unwrap())),
            8,
        ),
        0x21 | 0x22 if payload.len() >= 36 => (
            IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(&payload[..16]).unwrap(),
            )),
            IpAddr::V6(Ipv6Addr::from(
                <[u8; 16]>::try_from(&payload[16..32]).unwrap(),
            )),
            32,
        ),
        0x31 | 0x32 if payload.len() >= 216 => return Ok(None),
        _ => return Err(invalid()),
    };
    let port = |at| u16::from_be_bytes([payload[at], payload[at + 1]]);
    Ok(Some((
        SocketAddr::new(source, port(offset)),
        SocketAddr::new(destination, port(offset + 2)),
    )))
}
