//! Bounded TLS 1.3 post-handshake parsing and traffic-secret updates.
pub mod traffic;
use std::io;
#[derive(Default)]
pub struct TicketSink {
    pending: Vec<u8>,
    non_advancing: u8,
}
impl TicketSink {
    /// Validate complete ticket messages and discard them. Other handshake
    /// messages are rejected explicitly; encrypted peer input must never panic.
    pub fn receive(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.receive_record(bytes, true)?.is_some() {
            return Err(invalid());
        }
        Ok(())
    }

    /// Parse one complete TLS record. A returned flag requests a read-key
    /// update, and indicates whether the peer also requests a response.
    /// KeyUpdate must end at a record boundary (RFC 8446 section 5.1).
    pub fn receive_record(
        &mut self,
        bytes: &[u8],
        allow_tickets: bool,
    ) -> io::Result<Option<bool>> {
        if self.pending.len().saturating_add(bytes.len()) > 65536 {
            return Err(invalid());
        }
        self.pending.extend_from_slice(bytes);
        while self.pending.len() >= 4 {
            let length =
                u32::from_be_bytes([0, self.pending[1], self.pending[2], self.pending[3]]) as usize;
            let kind = self.pending[0];
            if !matches!(kind, 4 | 24)
                || (kind == 4 && !allow_tickets)
                || (kind == 24 && length != 1)
                || length > 65532
            {
                return Err(invalid());
            }
            if self.pending.len() < length + 4 {
                break;
            }
            self.ignored_message()?;
            if kind == 24 {
                if self.pending.len() != 5 || self.pending[4] > 1 {
                    return Err(invalid());
                }
                let requested = self.pending[4] == 1;
                self.pending.clear();
                return Ok(Some(requested));
            }
            ticket(&self.pending[4..4 + length])?;
            self.pending.drain(..4 + length);
        }
        Ok(None)
    }

    fn ignored_message(&mut self) -> io::Result<()> {
        // Pinned Go/REALITY default: at most 32 consecutive non-advancing messages.
        self.non_advancing = self.non_advancing.saturating_add(1);
        if self.non_advancing > 32 {
            Err(invalid())
        } else {
            Ok(())
        }
    }

    pub fn application_data(&mut self, nonempty: bool) -> io::Result<()> {
        self.ensure_complete()?;
        if nonempty {
            self.non_advancing = 0;
            Ok(())
        } else {
            self.ignored_message()
        }
    }

    /// Handshake fragments cannot be interleaved with application data.
    pub fn ensure_complete(&self) -> io::Result<()> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid or unsupported TLS post-handshake message",
    )
}
fn ticket(mut bytes: &[u8]) -> io::Result<()> {
    // ticket_lifetime, ticket_age_add, nonce<0..255>, ticket<1..65535>, extensions.
    if bytes.len() < 9 {
        return Err(invalid());
    }
    let nonce = bytes[8] as usize;
    bytes = bytes.get(9 + nonce..).ok_or_else(invalid)?;
    if bytes.len() < 2 {
        return Err(invalid());
    }
    let length = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    if length == 0 {
        return Err(invalid());
    }
    bytes = bytes.get(2 + length..).ok_or_else(invalid)?;
    if bytes.len() < 2 || u16::from_be_bytes([bytes[0], bytes[1]]) as usize + 2 != bytes.len() {
        return Err(invalid());
    }
    bytes = &bytes[2..];
    let mut seen = std::collections::HashSet::new();
    while !bytes.is_empty() {
        if bytes.len() < 4 {
            return Err(invalid());
        }
        let kind = u16::from_be_bytes([bytes[0], bytes[1]]);
        let length = u16::from_be_bytes([bytes[2], bytes[3]]) as usize;
        if !seen.insert(kind) || (kind == 42 && length != 4) {
            return Err(invalid());
        }
        bytes = bytes.get(4 + length..).ok_or_else(invalid)?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "../tests/unit/post_handshake.rs"]
mod tests;
