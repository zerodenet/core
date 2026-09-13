use alloc::string::String;

use zero_core::Error;

pub fn parse_uuid(input: &str) -> Result<[u8; 16], Error> {
    // Xray's custom ID uses UUIDv5 with the nil namespace. Hash the exact
    // UTF-8 bytes: whitespace is significant and the limit is bytes, not chars.
    if !(32..=36).contains(&input.len()) {
        if input.is_empty() || input.len() > 30 {
            return Err(Error::Config("VLESS ID must be a UUID or 1..=30 bytes"));
        }
        use sha1::{Digest, Sha1};
        let mut hash = Sha1::new();
        hash.update([0_u8; 16]);
        hash.update(input.as_bytes());
        let mut uuid: [u8; 16] = hash.finalize()[..16].try_into().unwrap();
        uuid[6] = (uuid[6] & 0x0f) | 0x50;
        uuid[8] = (uuid[8] & 0x3f) | 0x80;
        return Ok(uuid);
    }

    // Match the reference parser's optional separator at each group boundary.
    let mut text = input.as_bytes();
    let mut uuid = [0_u8; 16];
    let mut offset = 0;
    for length in [8, 4, 4, 4, 12] {
        if text.first() == Some(&b'-') {
            text = &text[1..];
        }
        let group = text
            .get(..length)
            .ok_or(Error::Config("VLESS UUID is truncated"))?;
        for pair in group.chunks_exact(2) {
            let high =
                hex_nibble(pair[0]).ok_or(Error::Config("VLESS UUID contains non-hex digits"))?;
            let low =
                hex_nibble(pair[1]).ok_or(Error::Config("VLESS UUID contains non-hex digits"))?;
            uuid[offset] = (high << 4) | low;
            offset += 1;
        }
        text = &text[length..];
    }
    Ok(uuid)
}

pub fn format_uuid(id: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (index, byte) in id.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push(hex_char(byte >> 4));
        out.push(hex_char(byte & 0x0f));
    }
    out
}

fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn hex_char(value: u8) -> char {
    match value {
        0..=9 => char::from(b'0' + value),
        10..=15 => char::from(b'a' + value - 10),
        _ => unreachable!("nibble value"),
    }
}
