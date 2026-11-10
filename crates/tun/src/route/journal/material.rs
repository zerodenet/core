//! Kernel material uses the same original Source's finite transient File slot.
use super::*;
mod paid;
pub(super) enum Bytes {
    // Only callers without an owning Source metadata input use existing bytes.
    Existing(Vec<u8>),
    Owned(crate::packet_memory::PacketMetadataVec<u8>),
}
impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Existing(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }
}
pub(super) fn read(
    path: &Path,
    memory: Option<&crate::packet_memory::PacketDriverMetadataView>,
) -> io::Result<Bytes> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_read(path) {
        return result.map(Bytes::Owned);
    }
    if let Some(memory) = memory {
        return paid::read(path, memory).map(Bytes::Owned);
    }
    std::fs::read(path).map(Bytes::Existing)
}
pub(super) fn encode(
    value: &impl Serialize,
    memory: Option<&crate::packet_memory::PacketDriverMetadataView>,
) -> io::Result<Bytes> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_encode(value) {
        return result.map(Bytes::Owned);
    }
    if let Some(memory) = memory {
        return paid::encode(value, memory).map(Bytes::Owned);
    }
    serde_json::to_vec(value)
        .map(Bytes::Existing)
        .map_err(io::Error::other)
}
pub(super) fn write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_write(path, bytes) {
        return result;
    }
    std::fs::write(path, bytes)
}
#[cfg(target_os = "macos")]
impl RouteJournal {
    pub(in crate::route) fn cleanup_context(
        &self,
    ) -> io::Result<Option<crate::MacosHelperContext>> {
        self.source
            .as_ref()
            .map(|source| source.context())
            .transpose()
    }
}
impl RouteJournal {
    pub(in crate::route) fn owned_closed(&self) -> bool {
        self._lease
            .as_ref()
            .is_some_and(RouteLease::physically_closed)
    }
    pub(in crate::route) fn close_owned(&mut self) -> io::Result<()> {
        match self._lease.as_mut() {
            Some(lease) => lease.close_retained(),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;

pub(super) fn read_from(
    path: &Path,
    memory: Option<&crate::packet_memory::PacketDriverMetadataView>,
    original: Option<&crate::route::file_originals::RouteFileIo>,
) -> io::Result<Bytes> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_read(path) {
        return result.map(Bytes::Owned);
    }
    if let Some(original) = original {
        return original.read(path).map(Bytes::Owned);
    }
    read(path, memory)
}
pub(super) fn write_from(
    path: &Path,
    bytes: &[u8],
    original: Option<&crate::route::file_originals::RouteFileIo>,
) -> io::Result<()> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_write(path, bytes) {
        return result;
    }
    if let Some(original) = original {
        return original.write(path, bytes);
    }
    write(path, bytes)
}
