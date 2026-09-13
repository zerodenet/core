//! Bounded plaintext ServerHello/HRR reassembly across TLS records.
use std::io;

pub(super) fn assemble(pending: &mut Vec<u8>, record: &[u8]) -> io::Result<Option<Vec<u8>>> {
    let invalid = || {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid ServerHello record framing",
        )
    };
    if record.len() < 5 {
        return Err(invalid());
    }
    let length = u16::from_be_bytes([record[3], record[4]]) as usize;
    if record.len() != length + 5 || length > 16384 {
        return Err(invalid());
    }
    if record[0] == 20 && length == 1 && record[5] == 1 {
        // RFC 8446 permits compatibility CCS after ClientHello, including before SH.
        // It cannot interrupt a fragmented handshake message.
        return if pending.is_empty() {
            Ok(None)
        } else {
            Err(invalid())
        };
    }
    if record[0] != 22 || length == 0 || pending.len() + length > u16::MAX as usize {
        return Err(invalid());
    }
    pending.extend_from_slice(&record[5..]);
    if pending[0] != 2 {
        return Err(invalid());
    }
    if pending.len() < 4 {
        return Ok(None);
    }
    let message_length =
        4 + ((pending[1] as usize) << 16) + ((pending[2] as usize) << 8) + pending[3] as usize;
    if message_length > u16::MAX as usize || pending.len() > message_length {
        return Err(invalid());
    }
    if pending.len() < message_length {
        return Ok(None);
    }
    // Existing parsers consume one synthetic record; the transcript uses only its payload.
    let mut result = vec![22, 3, 3];
    result.extend_from_slice(&(pending.len() as u16).to_be_bytes());
    result.append(pending);
    Ok(Some(result))
}

#[cfg(test)]
#[path = "../../tests/handshake/server_hello.rs"]
mod tests;
