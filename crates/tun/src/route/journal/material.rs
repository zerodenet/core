//! Kernel material uses the same original Source's finite transient File slot.
use super::*;
pub(super) fn read(path: &Path) -> io::Result<Vec<u8>> {
    #[cfg(target_os = "macos")]
    if let Some(result) = crate::macos_privilege::journal_read(path) {
        return result;
    }
    std::fs::read(path)
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
