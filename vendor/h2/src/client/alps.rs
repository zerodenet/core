//! Decode authenticated HTTP/2 ALPS without treating it as socket input.
use crate::{
    frame::{Head, Settings},
    Error, Reason,
};

pub(super) fn decode(mut input: &[u8]) -> Result<Vec<Settings>, Error> {
    let bad = || Error::from(Reason::PROTOCOL_ERROR);
    if input.len() > 65535 {
        return Err(bad());
    }
    let mut out = Vec::new();
    while !input.is_empty() {
        if input.len() < 9 {
            return Err(bad());
        }
        let n = ((input[0] as usize) << 16) | ((input[1] as usize) << 8) | input[2] as usize;
        if n > crate::frame::DEFAULT_MAX_FRAME_SIZE as usize {
            return Err(bad());
        }
        let body = input.get(9..9 + n).ok_or_else(bad)?;
        let head = Head::parse(&input[..9]);
        match input[3] {
            4 => {
                // A server must never send ENABLE_PUSH, including through ALPS.
                if head.flag() & 1 != 0 || body.chunks_exact(6).any(|s| s[..2] == [0, 2]) {
                    return Err(bad());
                }
                out.push(Settings::load(head, body).map_err(|_| bad())?);
            }
            0..=9 => return Err(bad()),
            0x89 => {
                // ACCEPT_CH hints are browser policy, not proxy request headers.
                // Validate the opaque origin/value pairs and leave them unused.
                if head.flag() != 0 || !head.stream_id().is_zero() {
                    return Err(bad());
                }
                let mut hints = body;
                while !hints.is_empty() {
                    for _ in 0..2 {
                        let prefix = hints.get(..2).ok_or_else(bad)?;
                        let n = u16::from_be_bytes([prefix[0], prefix[1]]) as usize;
                        hints = hints.get(2 + n..).ok_or_else(bad)?;
                    }
                }
            }
            _ => {} // HTTP/2 extension frames must be ignored when unknown.
        }
        input = &input[9 + n..];
    }
    Ok(out)
}
