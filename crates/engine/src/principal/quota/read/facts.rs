//! Storage facts only; the original serde decoder still accepts the book.
use std::io::{self, Read, Seek, SeekFrom};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct Facts {
    pub strings: usize,
    pub token: usize,
    pub objects: usize,
    pub nonwhite: usize,
}
#[derive(Default)]
struct Scan {
    facts: Facts,
    quoted: bool,
    escaped: bool,
    token: usize,
    bare_token: usize,
}
fn add(value: &mut usize, count: usize) -> io::Result<()> {
    *value = value.checked_add(count).ok_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidData, "quota decoded storage overflow")
    })?;
    Ok(())
}
impl Scan {
    fn feed(&mut self, bytes: &[u8]) -> io::Result<()> {
        for &byte in bytes {
            if self.quoted {
                // This is a raw upper bound for serde's UTF-8 decoded token.
                // Escaped quotes/backslashes remain within the same token.
                if !self.escaped && byte == b'"' {
                    self.quoted = false;
                    self.token = 0;
                } else {
                    add(&mut self.token, 1)?;
                    add(&mut self.facts.strings, 1)?;
                    self.facts.token = self.facts.token.max(self.token);
                    if self.escaped {
                        self.escaped = false;
                    } else if byte == b'\\' {
                        self.escaped = true;
                    }
                }
            } else {
                if !matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
                    add(&mut self.facts.nonwhite, 1)?;
                }
                if matches!(
                    byte,
                    b' ' | b'\t' | b'\n' | b'\r' | b'{' | b'}' | b'[' | b']' | b',' | b':' | b'"'
                ) {
                    self.bare_token = 0;
                } else {
                    add(&mut self.bare_token, 1)?;
                    self.facts.token = self.facts.token.max(self.bare_token);
                }
                match byte {
                    b'"' => {
                        self.quoted = true;
                        self.escaped = false;
                        self.token = 0;
                    }
                    b'{' => add(&mut self.facts.objects, 1)?,
                    _ => {}
                }
            }
        }
        Ok(())
    }
}
impl Facts {
    fn covers(self, actual: Self) -> bool {
        self.strings >= actual.strings
            && self.token >= actual.token
            && self.objects >= actual.objects
            && self.nonwhite >= actual.nonwhite
    }
    pub(super) fn measure(reader: &mut (impl Read + Seek)) -> io::Result<Self> {
        let mut scan = Scan::default();
        let mut bytes = [0; 4096];
        loop {
            let count = reader.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            scan.feed(&bytes[..count])?;
        }
        reader.seek(SeekFrom::Start(0))?;
        Ok(scan.facts)
    }
}
/// An external same-inode writer cannot grow decoding work beyond its prepaid
/// facts between passes: every chunk is remeasured before serde receives it.
/// This is not a second decoder and never accepts/replaces serde semantics.
pub(super) struct Guarded<R> {
    inner: R,
    prepaid: Facts,
    actual: Scan,
}
impl<R> Guarded<R> {
    pub(super) fn new(inner: R, prepaid: Facts) -> Self {
        Self {
            inner,
            prepaid,
            actual: Scan::default(),
        }
    }
}
impl<R: Read> Read for Guarded<R> {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = self.inner.read(bytes)?;
        self.actual.feed(&bytes[..count])?;
        if !self.prepaid.covers(self.actual.facts) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "quota snapshot changed beyond prepaid decoding storage",
            ));
        }
        Ok(count)
    }
}
