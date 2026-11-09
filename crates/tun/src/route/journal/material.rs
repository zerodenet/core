//! Kernel material uses the same original Source's finite transient File slot.
use super::*;
pub(super) enum Bytes {
    // Existing standalone and non-macOS callers still use their original Vec.
    // This includes Kernel callers; their Source budget is separate work.
    Existing(Vec<u8>),
    #[cfg(target_os = "macos")]
    Owned(crate::packet_memory::PacketMetadataVec<u8>),
}
impl std::ops::Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Existing(bytes) => bytes,
            #[cfg(target_os = "macos")]
            Self::Owned(bytes) => bytes,
        }
    }
}
pub(super) fn read(path: &Path) -> io::Result<Bytes> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_read(path) {
        return result.map(Bytes::Owned);
    }
    std::fs::read(path).map(Bytes::Existing)
}
pub(super) fn encode(value: &impl Serialize) -> io::Result<Bytes> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_encode(value) {
        return result.map(Bytes::Owned);
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
    pub(in crate::route) fn owned_closed(&self) -> bool {
        self._lease.as_ref().is_some_and(RouteLease::owned_closed)
    }
    pub(in crate::route) fn close_owned(&mut self) -> io::Result<()> {
        match self._lease.as_mut() {
            Some(lease) => lease.close_owned(),
            None => Ok(()),
        }
    }
}
