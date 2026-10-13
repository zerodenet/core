use serde::de::DeserializeOwned;
use std::io::{self, BufRead, BufReader, Cursor, Read, Seek, SeekFrom};

mod snapshot;
mod validation;
pub(crate) use snapshot::Snapshot;

pub(crate) const BUFFER_BYTES: usize = 64 * 1024;

#[cfg(test)]
mod tests;

pub(crate) struct Record<T> {
    pub(crate) parsed: Option<Result<T, serde_json::Error>>,
    pub(crate) bytes: u64,
    pub(crate) terminated: bool,
}

pub(crate) fn read_record<T: DeserializeOwned>(
    reader: &mut (impl BufRead + Seek),
    prefix: &mut Vec<u8>,
) -> io::Result<Option<Record<T>>> {
    let start = reader.stream_position()?;
    prefix.clear();
    let mut frame = Frame::new(reader);
    (&mut frame).take(BUFFER_BYTES as u64).read_to_end(prefix)?;
    if prefix.is_empty() {
        return Ok(None);
    }
    let mut parsed = if prefix == b"\n" {
        None
    } else if frame.terminated || prefix.len() < BUFFER_BYTES {
        Some(serde_json::from_slice(prefix))
    } else {
        Some(serde_json::from_reader(BufReader::with_capacity(
            BUFFER_BYTES,
            Cursor::new(&*prefix).chain(&mut frame),
        )))
    };
    if let Some(Err(error)) = &parsed {
        if error.is_io() {
            return Err(io::Error::new(
                error.io_error_kind().unwrap_or(io::ErrorKind::Other),
                error.to_string(),
            ));
        }
    }
    io::copy(&mut frame, &mut io::sink())?;
    // IgnoredAny skips payload allocation, so validate UTF-8 independently
    // even inside skipped strings and across buffer boundaries.
    if frame.invalid_utf8 || frame.continuations != 0 {
        parsed = Some(Err(<serde_json::Error as serde::de::Error>::custom(
            "invalid UTF-8 in JSONL record",
        )));
    }
    let bytes = frame.bytes;
    let terminated = frame.terminated;
    let short = terminated && bytes <= BUFFER_BYTES as u64 || bytes < BUFFER_BYTES as u64;
    if parsed.as_ref().is_some_and(Result::is_ok) {
        let validation = if short {
            validation::validate_slice(prefix)
        } else {
            reader.seek(SeekFrom::Start(start))?;
            let result = validation::validate_reader((&mut *reader).take(bytes));
            reader.seek(SeekFrom::Start(start + bytes))?;
            result
        };
        if let Err(error) = validation {
            if error.is_io() {
                return Err(io::Error::new(
                    error.io_error_kind().unwrap_or(io::ErrorKind::Other),
                    error.to_string(),
                ));
            }
            parsed = Some(Err(error));
        }
    }
    Ok(Some(Record {
        parsed,
        bytes,
        terminated,
    }))
}

// A JSONL frame is a streaming reader, not an allocation proportional to its
// length. The newline belongs to this reader, never to the following record.
pub(crate) struct Frame<'a, R> {
    reader: &'a mut R,
    pub(crate) bytes: u64,
    pub(crate) terminated: bool,
    invalid_utf8: bool,
    continuations: u8,
    next_min: u8,
    next_max: u8,
}

impl<'a, R: BufRead> Frame<'a, R> {
    pub(crate) fn new(reader: &'a mut R) -> Self {
        Self {
            reader,
            bytes: 0,
            terminated: false,
            invalid_utf8: false,
            continuations: 0,
            next_min: 0x80,
            next_max: 0xbf,
        }
    }

    fn check_utf8(&mut self, bytes: &[u8]) {
        if self.invalid_utf8 || (self.continuations == 0 && bytes.is_ascii()) {
            return;
        }
        for &byte in bytes {
            if self.continuations > 0 {
                if !(self.next_min..=self.next_max).contains(&byte) {
                    self.invalid_utf8 = true;
                    return;
                }
                self.continuations -= 1;
                self.next_min = 0x80;
                self.next_max = 0xbf;
            } else {
                let (count, min, max) = match byte {
                    0..=0x7f => continue,
                    0xc2..=0xdf => (1, 0x80, 0xbf),
                    0xe0 => (2, 0xa0, 0xbf),
                    0xe1..=0xec | 0xee..=0xef => (2, 0x80, 0xbf),
                    0xed => (2, 0x80, 0x9f),
                    0xf0 => (3, 0x90, 0xbf),
                    0xf1..=0xf3 => (3, 0x80, 0xbf),
                    0xf4 => (3, 0x80, 0x8f),
                    _ => {
                        self.invalid_utf8 = true;
                        return;
                    }
                };
                self.continuations = count;
                self.next_min = min;
                self.next_max = max;
            }
        }
    }
}

impl<R: BufRead> Read for Frame<'_, R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.terminated || output.is_empty() {
            return Ok(0);
        }
        let buffer = self.reader.fill_buf()?;
        let available = buffer.len().min(output.len());
        let size = buffer[..available]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available, |index| index + 1);
        output[..size].copy_from_slice(&buffer[..size]);
        self.terminated = size > 0 && buffer[size - 1] == b'\n';
        self.reader.consume(size);
        self.check_utf8(&output[..size]);
        self.bytes += size as u64;
        Ok(size)
    }
}

pub(crate) fn has_complete_frame(reader: &mut impl BufRead) -> io::Result<bool> {
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return Ok(false);
        }
        if buffer.contains(&b'\n') {
            return Ok(true);
        }
        let size = buffer.len();
        reader.consume(size);
    }
}
