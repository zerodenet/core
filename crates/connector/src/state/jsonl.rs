//! Existing JSONL validation over the same borrowed source snapshot.
use std::{
    io::{self, BufReader},
    path::Path,
    sync::Arc,
};
use zero_runtime::configuration::ConfigurationSource;
#[derive(Debug)]
struct RecordInvalid {
    index: u64,
    source: serde_json::Error,
}
impl std::fmt::Display for RecordInvalid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "record {} is invalid JSON: {}", self.index, self.source)
    }
}
impl std::error::Error for RecordInvalid {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
#[derive(Debug)]
struct ReadFailed(io::Error);
impl std::fmt::Display for ReadFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "read failed: {}", self.0)
    }
}
impl std::error::Error for ReadFailed {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.0)
    }
}
pub(super) fn inspect(
    path: &Path,
    source: &Arc<ConfigurationSource>,
) -> io::Result<(u64, io::Result<u64>)> {
    source.inspect_seekable_file_snapshot(path, |bytes, original| {
        let mut reader = BufReader::with_capacity(
            crate::jsonl::BUFFER_BYTES,
            crate::jsonl::Snapshot::new(original, bytes),
        );
        let mut prefix = Vec::with_capacity(crate::jsonl::BUFFER_BYTES);
        let mut records = 0;
        let mut index = 0;
        while let Some(frame) =
            crate::jsonl::read_record::<serde::de::IgnoredAny>(&mut reader, &mut prefix)
                .map_err(|source| io::Error::new(source.kind(), ReadFailed(source)))?
        {
            index += 1;
            let Some(parsed) = frame.parsed else { continue };
            parsed.map_err(|source| {
                io::Error::new(io::ErrorKind::InvalidData, RecordInvalid { index, source })
            })?;
            records += 1;
        }
        Ok(records)
    })
}
