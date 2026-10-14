//! A seekable view of the opened inode's initial length, without a writer lease.
use std::io::{self, Read, Seek, SeekFrom};

pub(crate) struct Snapshot<R> {
    reader: R,
    length: u64,
    position: u64,
}

impl<R> Snapshot<R> {
    pub(crate) fn new(reader: R, length: u64) -> Self {
        Self {
            reader,
            length,
            position: 0,
        }
    }
}

impl<R: Read> Read for Snapshot<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let available = self.length.saturating_sub(self.position);
        let limit = available.min(output.len() as u64) as usize;
        let size = self.reader.read(&mut output[..limit])?;
        self.position += size as u64;
        Ok(size)
    }
}

impl<R: Seek> Seek for Snapshot<R> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let offset = match target {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
            SeekFrom::End(offset) => i128::from(self.length) + i128::from(offset),
        };
        if offset < 0 || offset > i128::from(self.length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek outside JSONL snapshot",
            ));
        }
        self.position = self.reader.seek(SeekFrom::Start(offset as u64))?;
        Ok(self.position)
    }
}
