use super::*;
use serde::{
    de::{Error, Visitor},
    Deserialize,
};
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Key {
    Runtime,
    File,
    Request,
    Magic,
    Other,
}
impl<'de> Deserialize<'de> for Key {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Name;
        impl<'de> Visitor<'de> for Name {
            type Value = Key;
            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("budget fact key")
            }
            fn visit_str<E: Error>(self, s: &str) -> Result<Key, E> {
                Ok(match s {
                    "runtime" => Key::Runtime,
                    "file_read_max_bytes" => Key::File,
                    "control_max_in_flight_request_bytes" => Key::Request,
                    "$serde_json::private::RawValue" => Key::Magic,
                    _ => Key::Other,
                })
            }
        }
        d.deserialize_str(Name)
    }
}
pub(super) struct Scan<'a> {
    reader: &'a mut dyn Read,
    buffer: [u8; 4096],
    start: usize,
    end: usize,
}
impl<'a> Scan<'a> {
    pub(super) fn new(reader: &'a mut dyn Read) -> Self {
        Self {
            reader,
            buffer: [0; 4096],
            start: 0,
            end: 0,
        }
    }
    pub(super) fn peek(&mut self) -> io::Result<Option<u8>> {
        if self.start == self.end {
            self.start = 0;
            self.end = loop {
                match self.reader.read(&mut self.buffer) {
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    result => break result?,
                }
            };
        }
        Ok((self.start < self.end).then(|| self.buffer[self.start]))
    }
    pub(super) fn take(&mut self) -> io::Result<Option<u8>> {
        let c = self.peek()?;
        if c.is_some() {
            self.start += 1
        }
        Ok(c)
    }
    pub(super) fn ws(&mut self) -> io::Result<()> {
        while matches!(self.peek()?, Some(b' ' | b'\r' | b'\n' | b'\t')) {
            self.take()?;
        }
        Ok(())
    }
    pub(super) fn bom(&mut self) -> io::Result<()> {
        if self.peek()? == Some(0xef) {
            for b in [0xef, 0xbb, 0xbf] {
                if self.take()? != Some(b) {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
    pub(super) fn colon(&mut self) -> io::Result<()> {
        self.ws()?;
        if self.take()? != Some(b':') {
            return Err(invalid());
        }
        Ok(())
    }
    pub(super) fn key(&mut self) -> io::Result<Key> {
        self.ws()?;
        if self.take()? != Some(b'"') {
            return Err(invalid());
        }
        // Longest recognized ASCII key may spell each byte as six-byte \uXXXX.
        // Unknown keys never allocate and are skipped even when much longer.
        let mut token = [0u8; 6 * "control_max_in_flight_request_bytes".len() + 2];
        let mut n = 1usize;
        token[0] = b'"';
        let mut escaped = false;
        let mut large = false;
        loop {
            let c = self.take()?.ok_or_else(invalid)?;
            if n < token.len() {
                token[n] = c;
                n += 1
            } else {
                large = true
            }
            if escaped {
                escaped = false;
                continue;
            }
            if c == b'\\' {
                escaped = true
            } else if c == b'"' {
                break;
            }
        }
        if large {
            return Ok(Key::Other);
        }
        serde_json::from_slice(&token[..n]).map_err(unknown)
    }
    pub(super) fn skip(&mut self) -> io::Result<()> {
        self.ws()?;
        match self.peek()?.ok_or_else(invalid)? {
            b'"' => {
                self.take()?;
                self.string()?;
            }
            b'{' | b'[' => {
                let mut depth = 0usize;
                loop {
                    match self.take()?.ok_or_else(invalid)? {
                        b'"' => self.string()?,
                        b'{' | b'[' => depth = depth.checked_add(1).ok_or_else(invalid)?,
                        b'}' | b']' => {
                            depth = depth.checked_sub(1).ok_or_else(invalid)?;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {
                let mut any = false;
                while !matches!(
                    self.peek()?,
                    None | Some(b',' | b'}' | b']' | b' ' | b'\r' | b'\n' | b'\t')
                ) {
                    self.take()?;
                    any = true;
                }
                if !any {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
    fn string(&mut self) -> io::Result<()> {
        let mut escaped = false;
        loop {
            let c = self.take()?.ok_or_else(invalid)?;
            if escaped {
                escaped = false;
                continue;
            }
            if c == b'\\' {
                escaped = true
            } else if c == b'"' {
                return Ok(());
            }
        }
    }
    pub(super) fn bound(&mut self, request: bool) -> io::Result<Option<usize>> {
        self.ws()?;
        if matches!(self.peek()?, Some(b'"' | b'{' | b'[')) {
            self.skip()?;
            return Err(budget_unknown(
                <serde_json::Error as serde::de::Error>::custom(
                    "initial IO-budget fact is not an unsigned integer or null",
                ),
            ));
        }
        let mut token = [0u8; 32];
        let mut n = 0;
        let mut large = false;
        while !matches!(
            self.peek()?,
            None | Some(b',' | b'}' | b']' | b' ' | b'\r' | b'\n' | b'\t')
        ) {
            let c = self.take()?.unwrap();
            if n < token.len() {
                token[n] = c;
                n += 1
            } else {
                large = true
            }
        }
        if large {
            return Err(budget_unknown(
                <serde_json::Error as serde::de::Error>::custom(
                    "initial IO-budget integer is out of range",
                ),
            ));
        }
        if &token[..n] == b"null" {
            return Ok(None);
        }
        let value = serde_json::from_slice::<usize>(&token[..n]).map_err(budget_unknown)?;
        if value == 0 || value.checked_add(1).is_none() || (request && value > 256 * 1024 * 1024) {
            return Err(budget_unknown(
                <serde_json::Error as serde::de::Error>::custom(
                    "initial IO-budget integer is outside its configured range",
                ),
            ));
        }
        Ok(Some(value))
    }
}
impl Read for Scan<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut used = 0;
        while used < out.len() {
            let Some(c) = self.take()? else { break };
            out[used] = c;
            used += 1;
        }
        Ok(used)
    }
}
