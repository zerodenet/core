//! Lazy Value-magic string view. Each escape is decoded by the actual serde
//! char decoder from at most a twelve-byte surrogate pair, never a whole DTO.
use super::*;
pub(super) struct Decoded<'a, 'b> {
    input: &'a mut Scan<'b>,
    pending: [u8; 4],
    used: usize,
    end: usize,
    closed: bool,
}
impl<'a, 'b> Decoded<'a, 'b> {
    pub(super) fn new(input: &'a mut Scan<'b>) -> Self {
        Self {
            input,
            pending: [0; 4],
            used: 0,
            end: 0,
            closed: false,
        }
    }
    pub(super) fn done(&self) -> bool {
        self.closed
    }
    fn byte(&mut self) -> io::Result<Option<u8>> {
        if self.used < self.end {
            let c = self.pending[self.used];
            self.used += 1;
            return Ok(Some(c));
        }
        if self.closed {
            return Ok(None);
        }
        let c = self.input.take()?.ok_or_else(invalid)?;
        if c == b'"' {
            self.closed = true;
            return Ok(None);
        }
        if c != b'\\' {
            return Ok(Some(c));
        }
        let mut token = [0u8; 14];
        token[0] = b'"';
        token[1] = b'\\';
        let e = self.input.take()?.ok_or_else(invalid)?;
        token[2] = e;
        let mut n = 3;
        if e == b'u' {
            for _ in 0..4 {
                token[n] = self.input.take()?.ok_or_else(invalid)?;
                n += 1;
            }
            let hex = std::str::from_utf8(&token[3..7]).map_err(|_| invalid())?;
            let code = u16::from_str_radix(hex, 16).map_err(|_| invalid())?;
            if (0xd800..=0xdbff).contains(&code) {
                for _ in 0..6 {
                    token[n] = self.input.take()?.ok_or_else(invalid)?;
                    n += 1;
                }
            }
        }
        token[n] = b'"';
        n += 1;
        let scalar = serde_json::from_slice::<char>(&token[..n]).map_err(unknown)?;
        self.end = scalar.encode_utf8(&mut self.pending).len();
        self.used = 1;
        Ok(Some(self.pending[0]))
    }
}
impl Read for Decoded<'_, '_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut n = 0;
        while n < out.len() {
            let Some(c) = self.byte()? else { break };
            out[n] = c;
            n += 1;
        }
        Ok(n)
    }
}
