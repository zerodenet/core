use std::io::{self, BufRead, Read};

pub(super) struct StringElider<R> {
    reader: R,
    inside: bool,
}

impl<R: BufRead> StringElider<R> {
    pub(super) fn new(reader: R) -> Self {
        Self {
            reader,
            inside: false,
        }
    }

    fn byte(&mut self) -> io::Result<Option<u8>> {
        let value = self.reader.fill_buf()?.first().copied();
        if value.is_some() {
            self.reader.consume(1);
        }
        Ok(value)
    }

    fn required(&mut self) -> io::Result<u8> {
        self.byte()?
            .ok_or_else(|| invalid("incomplete JSON string"))
    }

    fn hex_quad(&mut self) -> io::Result<u16> {
        let mut value = 0;
        for _ in 0..4 {
            let digit = match self.required()? {
                ch @ b'0'..=b'9' => ch - b'0',
                ch @ b'a'..=b'f' => ch - b'a' + 10,
                ch @ b'A'..=b'F' => ch - b'A' + 10,
                _ => return Err(invalid("invalid Unicode escape in JSON string")),
            };
            value = (value << 4) | u16::from(digit);
        }
        Ok(value)
    }

    fn escape(&mut self) -> io::Result<()> {
        match self.required()? {
            b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't' => Ok(()),
            b'u' => {
                let first = self.hex_quad()?;
                if (0xd800..=0xdbff).contains(&first) {
                    if self.required()? != b'\\' || self.required()? != b'u' {
                        return Err(invalid("unpaired leading Unicode surrogate"));
                    }
                    if !(0xdc00..=0xdfff).contains(&self.hex_quad()?) {
                        return Err(invalid("invalid trailing Unicode surrogate"));
                    }
                } else if (0xdc00..=0xdfff).contains(&first) {
                    return Err(invalid("unpaired trailing Unicode surrogate"));
                }
                Ok(())
            }
            _ => Err(invalid("invalid escape in JSON string")),
        }
    }

    fn skip_string(&mut self) -> io::Result<()> {
        loop {
            let buffer = self.reader.fill_buf()?;
            if buffer.is_empty() {
                return Err(invalid("incomplete JSON string"));
            }
            let size = buffer
                .iter()
                .position(|ch| *ch == b'"' || *ch == b'\\' || *ch < 0x20)
                .unwrap_or(buffer.len());
            self.reader.consume(size);
            if size != 0 {
                continue;
            }
            match self.required()? {
                b'"' => return Ok(()),
                b'\\' => self.escape()?,
                _ => return Err(invalid("control character in JSON string")),
            }
        }
    }
}

impl<R: BufRead> Read for StringElider<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.inside {
            self.skip_string()?;
            self.inside = false;
            output[0] = b'"';
            return Ok(1);
        }
        let Some(ch) = self.byte()? else {
            return Ok(0);
        };
        self.inside = ch == b'"';
        output[0] = ch;
        Ok(1)
    }
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
