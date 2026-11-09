//! Same existing TUN ClientHello grammar, with no temporary SNI String.
use super::MAX_CLIENT_HELLO_LENGTH;
use crate::runtime::sniff::inline::InlineName;
pub(super) fn sniff_tls_name(handshake: &[u8]) -> Option<InlineName> {
    if handshake.len() < 4 || handshake[0] != 1 {
        return None;
    }
    let length =
        ((handshake[1] as usize) << 16) | ((handshake[2] as usize) << 8) | handshake[3] as usize;
    if length > MAX_CLIENT_HELLO_LENGTH {
        return None;
    }
    let hello = handshake.get(4..4 + length)?;
    let mut offset = 34;
    let session = usize::from(*take(hello, &mut offset, 1)?.first()?);
    take(hello, &mut offset, session)?;
    let ciphers = u16::from_be_bytes(take(hello, &mut offset, 2)?.try_into().ok()?) as usize;
    take(hello, &mut offset, ciphers)?;
    let compression = usize::from(*take(hello, &mut offset, 1)?.first()?);
    take(hello, &mut offset, compression)?;
    if offset == hello.len() {
        return None;
    }
    let length = u16::from_be_bytes(take(hello, &mut offset, 2)?.try_into().ok()?) as usize;
    let extensions = take(hello, &mut offset, length)?;
    let (mut offset, mut name, mut ech) = (0, None, false);
    while offset + 4 <= extensions.len() {
        let kind = u16::from_be_bytes([extensions[offset], extensions[offset + 1]]);
        let length = u16::from_be_bytes([extensions[offset + 2], extensions[offset + 3]]) as usize;
        offset += 4;
        let Some(extension) = extensions.get(offset..offset + length) else {
            break;
        };
        if kind == 0 && extension.len() >= 5 && extension[2] == 0 {
            let size = u16::from_be_bytes([extension[3], extension[4]]) as usize;
            if let Some(bytes) = extension.get(5..5 + size) {
                name = std::str::from_utf8(bytes).ok();
            }
        } else if kind == 0xfe0d {
            ech = true;
        }
        offset += length;
    }
    if ech {
        None
    } else {
        name.and_then(InlineName::new)
    }
}
fn take<'a>(bytes: &'a [u8], offset: &mut usize, length: usize) -> Option<&'a [u8]> {
    let end = offset.checked_add(length)?;
    let value = bytes.get(*offset..end)?;
    *offset = end;
    Some(value)
}

#[cfg(test)]
mod tests;
