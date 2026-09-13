//! Validate the server's authenticated ALPN selection.
use crate::buf_reader::BufReader;
use std::io;

pub(super) fn alpn(message: &[u8], offered: &[String]) -> io::Result<Option<Vec<u8>>> {
    let invalid = || io::Error::new(io::ErrorKind::InvalidData, "invalid encrypted extensions");
    let mut r = BufReader::new(message);
    if r.read_u8()? != 8 {
        return Err(invalid());
    }
    let length = r.read_u24_be()? as usize;
    let mut r = BufReader::new(r.read_slice(length)?);
    let length = r.read_u16_be()? as usize;
    if length != r.remaining() {
        return Err(invalid());
    }
    let mut seen = std::collections::HashSet::new();
    let mut selected = None;
    while !r.is_consumed() {
        let kind = r.read_u16_be()?;
        let length = r.read_u16_be()? as usize;
        let data = r.read_slice(length)?;
        if !seen.insert(kind) {
            return Err(invalid());
        }
        if kind == 16 {
            let mut value = BufReader::new(data);
            let length = value.read_u16_be()? as usize;
            if length != value.remaining() {
                return Err(invalid());
            }
            let length = value.read_u8()? as usize;
            let protocol = value.read_slice(length)?;
            if length == 0
                || !value.is_consumed()
                || !offered.iter().any(|name| name.as_bytes() == protocol)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "server selected an unoffered ALPN",
                ));
            }
            selected = Some(protocol.to_vec());
        }
    }
    Ok(selected)
}

/// Read the final wire offer, including presets that deliberately omit ALPN.
pub(super) fn offered_alpn(hello: &[u8]) -> io::Result<Vec<String>> {
    let (_, extensions) = crate::fingerprint::wire::parts(hello)?;
    let Some((_, bytes)) = extensions.iter().find(|(kind, _)| *kind == 16) else {
        return Ok(Vec::new());
    };
    let mut r = BufReader::new(bytes);
    let length = r.read_u16_be()? as usize;
    if length != r.remaining() {
        return Err(io::Error::other("invalid ClientHello ALPN"));
    }
    let mut result = Vec::new();
    while !r.is_consumed() {
        let n = r.read_u8()? as usize;
        if n == 0 {
            return Err(io::Error::other("empty ClientHello ALPN"));
        }
        result.push(String::from_utf8(r.read_slice(n)?.to_vec()).map_err(io::Error::other)?);
    }
    Ok(result)
}

#[cfg(test)]
#[path = "../../tests/handshake/alpn.rs"]
mod tests;
