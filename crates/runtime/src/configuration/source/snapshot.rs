//! A read-only bounded view of the same configuration source's original inode.
use super::ConfigurationSource;
use std::{
    io::{self, Read, Seek, SeekFrom},
    path::Path,
    sync::Arc,
};
use zero_platform_tokio::file::OriginalFile;
/// Borrowed read/seek only. It cannot clone, write, close or export the original.
pub struct ConfigurationSnapshotReader<'a> {
    original: &'a mut OriginalFile,
    length: u64,
    position: u64,
}
impl Read for ConfigurationSnapshotReader<'_> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let count = self
            .length
            .saturating_sub(self.position)
            .min(out.len() as u64) as usize;
        let count = self.original.read(&mut out[..count])?;
        self.position += count as u64;
        Ok(count)
    }
}
impl Seek for ConfigurationSnapshotReader<'_> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let position = match target {
            SeekFrom::Start(offset) => i128::from(offset),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
            SeekFrom::End(offset) => i128::from(self.length) + i128::from(offset),
        };
        if position < 0 || position > i128::from(self.length) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "seek outside original configuration snapshot",
            ));
        }
        self.position = self.original.seek(SeekFrom::Start(position as u64))?;
        Ok(self.position)
    }
}
impl ConfigurationSource {
    /// Inspect this actual inode through the existing short-file/startup slot.
    /// The result follows matching original close; decode errors are separate
    /// from an unknown close. Appends cannot extend the captured read bound.
    pub fn inspect_seekable_file_snapshot<T>(
        self: &Arc<Self>,
        path: &Path,
        decode: impl FnOnce(u64, &mut ConfigurationSnapshotReader<'_>) -> io::Result<T>,
    ) -> io::Result<(u64, io::Result<T>)> {
        super::super::startup::with_source(self.clone(), || {
            self.ensure_resource_io_available()?;
            self.io.with_file(
                || std::fs::File::open(path),
                |original| {
                    let length = original.metadata()?.len();
                    let mut snapshot = ConfigurationSnapshotReader {
                        original,
                        length,
                        position: 0,
                    };
                    Ok((length, decode(length, &mut snapshot)))
                },
            )
        })?
    }
}
#[cfg(test)]
#[path = "../../tests/configuration_snapshot.rs"]
mod tests;
