//! One pending family lease; its pure fault delivery owns no native resource.
use super::{journal::RouteLease, *};
mod faults;
mod original;
mod transient;
pub(in crate::route) use faults::Attempt;
pub use faults::RouteFileFaults;
pub(in crate::route) use original::TrackedFile;
pub(in crate::route) use transient::RouteFileIo;

#[derive(Debug)]
pub struct RouteFileOriginals {
    pub(in crate::route) pending: Option<RouteLease>,
    pub(in crate::route) faults: RouteFileFaults,
}
impl RouteFileOriginals {
    pub fn new(faults: RouteFileFaults) -> Self {
        Self {
            pending: None,
            faults,
        }
    }
    /// Custody observation only. The empty slot is not native-close evidence.
    pub fn has_pending(&self) -> bool {
        self.pending.is_some()
    }
    /// Uses only the original operation's prepaid close cells. Pausing new
    /// metadata admission cannot prevent cleanup of an existing descriptor.
    pub fn retire_pending(&mut self) -> io::Result<()> {
        if let Some(lease) = self.pending.as_mut() {
            lease.close_retained()?;
        }
        self.pending.take();
        Ok(())
    }
}
#[cfg(test)]
mod tests;
