//! The one process backend retained by each existing protocol lease.
#[cfg(target_os = "linux")]
use std::sync::Arc;
use std::{io, process::ExitStatus};
use tokio::process::Child;
/// Standalone library plans retain their explicit direct-child responsibility.
/// Linux Host plans retain the Host's same original process tree instead.
pub(crate) enum PluginChild {
    Library(Child),
    #[cfg(target_os = "linux")]
    Owned(Arc<zero_platform_tokio::owned_process::OwnedProcess>),
}
impl PluginChild {
    pub(crate) fn try_wait(&mut self) -> io::Result<Option<ExitStatus>> {
        match self {
            Self::Library(child) => child.try_wait(),
            #[cfg(target_os = "linux")]
            Self::Owned(original) => {
                if let Some(status) = original.root_status() {
                    return Ok(Some(status));
                }
                if let Some(source) = original.first_failure() {
                    return Err(io::Error::new(source.kind(), OriginalFailure(source)));
                }
                Ok(None)
            }
        }
    }
    pub(crate) fn start_kill(&mut self) -> io::Result<()> {
        match self {
            Self::Library(child) => child.start_kill(),
            #[cfg(target_os = "linux")]
            Self::Owned(original) => {
                original.request_stop();
                Ok(())
            }
        }
    }
    #[cfg(any(test, not(target_os = "linux")))]
    pub(crate) fn id(&self) -> Option<u32> {
        match self {
            Self::Library(child) => child.id(),
            #[cfg(target_os = "linux")]
            Self::Owned(_) => None,
        }
    }
    #[cfg(target_os = "linux")]
    pub(crate) fn owned(&self) -> Option<Arc<zero_platform_tokio::owned_process::OwnedProcess>> {
        match self {
            Self::Owned(original) => Some(original.clone()),
            _ => None,
        }
    }
}
#[cfg(target_os = "linux")]
#[derive(Debug)]
struct OriginalFailure(Arc<io::Error>);
#[cfg(target_os = "linux")]
impl std::fmt::Display for OriginalFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
#[cfg(target_os = "linux")]
impl std::error::Error for OriginalFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
