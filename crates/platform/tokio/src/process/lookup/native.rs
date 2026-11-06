//! Platform-specific actual originals; job joins and native receipts are separate.
use super::*;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "linux")]
pub(in crate::process) use linux::Native;
#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(in crate::process) use windows::Native;
#[cfg(not(any(target_os = "linux", windows)))]
pub(in crate::process) struct Native;
#[cfg(not(any(target_os = "linux", windows)))]
impl Native {
    pub(super) fn new() -> Self {
        Self
    }
    pub(super) fn confirmed(&self) -> bool {
        true
    }
    pub(super) fn failure(&self) -> Option<Arc<ChannelError>> {
        None
    }
    pub(super) fn collect_after_join(&self) -> Result<(), Arc<ChannelError>> {
        Ok(())
    }
}
