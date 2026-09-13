//! Bounded ClientHello inspection across TLS record and socket read boundaries.
use super::{client_hello::parse_extensions, InboundClientHello};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt};
pub async fn peek_client_hello<R: AsyncRead + Unpin>(
    reader: &mut R,
) -> io::Result<Option<InboundClientHello>> {
    let mut consumed = Vec::new();
    let mut handshake = Vec::new();
    loop {
        let mut header = [0u8; 5];
        reader.read_exact(&mut header).await?;
        consumed.extend(header);
        if header[0] != 22 {
            return Ok(Some(InboundClientHello {
                consumed,
                ..Default::default()
            }));
        }
        let length = usize::from(u16::from_be_bytes([header[3], header[4]]));
        if length > 18432 || consumed.len() + length > 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized TLS ClientHello records",
            ));
        }
        let start = consumed.len();
        consumed.resize(start + length, 0);
        reader.read_exact(&mut consumed[start..]).await?;
        handshake.extend_from_slice(&consumed[start..]);
        if handshake.len() < 4 {
            continue;
        }
        if handshake[0] != 1 {
            return Ok(Some(InboundClientHello {
                consumed,
                ..Default::default()
            }));
        }
        let length = (usize::from(handshake[1]) << 16)
            | (usize::from(handshake[2]) << 8)
            | usize::from(handshake[3]);
        if length > 65536 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "oversized TLS ClientHello",
            ));
        }
        if handshake.len() < length + 4 {
            continue;
        }
        let mut hello = extensions(&handshake[4..length + 4])
            .map(|extensions| parse_extensions(extensions, Vec::new()))
            .unwrap_or_default();
        hello.consumed = consumed;
        return Ok(Some(hello));
    }
}
fn extensions(mut body: &[u8]) -> Option<&[u8]> {
    fn skip<'a>(body: &mut &'a [u8], length: usize) -> Option<&'a [u8]> {
        let head = body.get(..length)?;
        *body = &body[length..];
        Some(head)
    }
    let session = usize::from(*skip(&mut body, 35)?.last()?);
    skip(&mut body, session)?;
    let cipher_length = u16::from_be_bytes(skip(&mut body, 2)?.try_into().ok()?) as usize;
    skip(&mut body, cipher_length)?;
    let compression = usize::from(skip(&mut body, 1)?[0]);
    skip(&mut body, compression)?;
    let length = u16::from_be_bytes(skip(&mut body, 2)?.try_into().ok()?) as usize;
    let extensions = skip(&mut body, length)?;
    body.is_empty().then_some(extensions)
}
#[cfg(test)]
#[path = "../../tests/tls/peek.rs"]
mod tests;
