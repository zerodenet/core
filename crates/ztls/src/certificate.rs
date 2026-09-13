//! Bounded TLS 1.3 server Certificate message parsing, shared by TLS and REALITY.
pub mod compression;
use std::{collections::HashSet, io};
mod verify;
pub use verify::{ServerTrust, VerifiedServer};

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid TLS server Certificate message",
    )
}
fn u24(bytes: &[u8]) -> usize {
    u32::from_be_bytes([0, bytes[0], bytes[1], bytes[2]]) as usize
}

/// Validate the entire message, including every certificate's extension vector.
/// Server authentication has an empty request context. Returned DER slices borrow
/// the original message so REALITY can authenticate without duplicating the chain.
pub fn server_chain(message: &[u8]) -> io::Result<Vec<&[u8]>> {
    if message.len() < 8
        || message.len() > 262144
        || message[0] != 11
        || u24(&message[1..4]) + 4 != message.len()
        || message[4] != 0
        || u24(&message[5..8]) + 8 != message.len()
    {
        return Err(invalid());
    }
    let mut entries = &message[8..];
    let mut chain = Vec::new();
    while !entries.is_empty() {
        if entries.len() < 3 {
            return Err(invalid());
        }
        let length = u24(entries);
        if length == 0 || length + 5 > entries.len() {
            return Err(invalid());
        }
        chain.push(&entries[3..3 + length]);
        entries = &entries[3 + length..];
        let length = u16::from_be_bytes([entries[0], entries[1]]) as usize;
        if length + 2 > entries.len() {
            return Err(invalid());
        }
        let mut extensions = &entries[2..2 + length];
        let mut seen = HashSet::new();
        while !extensions.is_empty() {
            if extensions.len() < 4 {
                return Err(invalid());
            }
            let kind = u16::from_be_bytes([extensions[0], extensions[1]]);
            let length = u16::from_be_bytes([extensions[2], extensions[3]]) as usize;
            if length + 4 > extensions.len() || !seen.insert(kind) {
                return Err(invalid());
            }
            extensions = &extensions[4 + length..];
        }
        entries = &entries[2 + length..];
    }
    if chain.is_empty() {
        return Err(invalid());
    }
    Ok(chain)
}

#[cfg(test)]
#[path = "../tests/unit/certificate.rs"]
mod tests;
