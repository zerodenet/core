//! The actual Source journal's encoded bytes retain their own paid array.
use super::*;
use crate::packet_memory::{PacketMemoryView, PacketMetadataVec};
use std::io::Write;
struct Writer<'a> {
    memory: &'a PacketMemoryView,
    limit: usize,
    bytes: PacketMetadataVec<u8>,
}
impl Write for Writer<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|len| len > self.limit)
        {
            return Err(io::Error::other(
                "route journal exceeds actual file material allowance",
            ));
        }
        for &byte in bytes {
            self.bytes
                .try_push(byte, self.memory, self.limit)
                .map_err(|(source, _)| io::Error::other(source))?;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl MacosHelperOwner {
    pub(super) fn encode_journal(
        &self,
        value: &impl serde::Serialize,
    ) -> io::Result<PacketMetadataVec<u8>> {
        if !self.0.io_allowed() {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        let mut writer = Writer {
            memory: &self.0.memory,
            limit: self.0.output_limit,
            bytes: Default::default(),
        };
        serde_json::to_writer(&mut writer, value).map_err(io::Error::other)?;
        Ok(writer.bytes)
    }
}
