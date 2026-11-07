//! Streaming getdents cursor, with byte limits on the same retained DIR File.
use super::*;
pub(super) struct Cursor {
    bytes: Vec<u8>,
    offset: usize,
    length: usize,
    consumed: usize,
    limit: usize,
}
impl Cursor {
    pub(super) fn new(limit: usize) -> Result<Self, Arc<ChannelError>> {
        // getdents must be able to hold an actual kernel dirent. This is a
        // minimum operation size, never an exemption from the configured cap.
        if limit < std::mem::size_of::<libc::dirent>() {
            return Err(capacity());
        }
        Ok(Self {
            bytes: vec![0; limit.min(8192)],
            offset: 0,
            length: 0,
            consumed: 0,
            limit,
        })
    }
    pub(super) fn next(
        &mut self,
        original: &mut Originals,
        slot: usize,
    ) -> Result<Option<String>, Arc<ChannelError>> {
        loop {
            if self.offset == self.length {
                let remaining = self.limit.saturating_sub(self.consumed);
                let size = self.bytes.len().min(remaining);
                self.length =
                    original
                        .directory(slot, &mut self.bytes[..size])
                        .map_err(|source| {
                            if source.raw_os_error() == Some(libc::EINVAL) {
                                capacity()
                            } else {
                                io_failure(source)
                            }
                        })?;
                self.offset = 0;
                self.consumed += self.length;
                if self.length == 0 {
                    return Ok(None);
                }
            }
            let remaining = &self.bytes[self.offset..self.length];
            if remaining.len() < 19 {
                return Err(invalid());
            }
            let length = usize::from(u16::from_ne_bytes([remaining[16], remaining[17]]));
            if length < 20 || length > remaining.len() {
                return Err(invalid());
            }
            let bytes = &remaining[19..length];
            let length_name = bytes
                .iter()
                .position(|byte| *byte == 0)
                .ok_or_else(invalid)?;
            let name = std::str::from_utf8(&bytes[..length_name])
                .map_err(|_| invalid())?
                .to_owned();
            self.offset += length;
            if name != "." && name != ".." {
                return Ok(Some(name));
            }
        }
    }
}
