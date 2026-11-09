//! Existing non-helper File operations, with the owning Source's actual bytes.
use super::*;
use crate::packet_memory::{PacketDriverMetadataView, PacketMetadataVec};
use std::io::{Read, Write};
pub(super) fn read(
    path: &Path,
    memory: &PacketDriverMetadataView,
) -> io::Result<PacketMetadataVec<u8>> {
    let mut file = std::fs::File::open(path)?;
    let mut result = PacketMetadataVec::default();
    loop {
        let mut scratch = [0; 4096];
        let count = match file.read(&mut scratch) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            result => result?,
        };
        if count == 0 {
            return Ok(result);
        }
        for &byte in &scratch[..count] {
            result
                .try_push_with_metadata(byte, memory, usize::MAX)
                .map_err(|(error, _)| io::Error::other(error))?;
        }
    }
}
struct Writer<'a> {
    memory: &'a PacketDriverMetadataView,
    bytes: PacketMetadataVec<u8>,
}
impl Write for Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        for &byte in bytes {
            self.bytes
                .try_push_with_metadata(byte, self.memory, usize::MAX)
                .map_err(|(error, _)| io::Error::other(error))?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
pub(super) fn encode(
    value: &impl Serialize,
    memory: &PacketDriverMetadataView,
) -> io::Result<PacketMetadataVec<u8>> {
    let mut writer = Writer {
        memory,
        bytes: Default::default(),
    };
    serde_json::to_writer(&mut writer, value).map_err(io::Error::other)?;
    Ok(writer.bytes)
}
