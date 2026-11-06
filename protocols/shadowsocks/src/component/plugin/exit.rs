//! The actual process provider's root outcome, preserved as a typed source.
use std::{fmt, process::ExitStatus};
#[derive(Clone, Copy, Debug)]
pub struct PluginRootExit {
    status: ExitStatus,
}
impl PluginRootExit {
    pub(super) fn new(status: ExitStatus) -> Self {
        Self { status }
    }
    pub fn status(&self) -> ExitStatus {
        self.status
    }
}
impl fmt::Display for PluginRootExit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "native plugin child exited: {}", self.status)
    }
}
impl std::error::Error for PluginRootExit {}
