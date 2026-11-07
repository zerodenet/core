//! Same actual Resource receiver, also usable before whole-owner shutdown.
use super::*;
pub(super) use zero_platform_tokio::contract::lifecycle::CleanupFailurePhase as Phase;
impl Resource {
    pub(super) fn receive_cleanup_failure(&self, error: ChannelError, phase: Phase) -> SharedError {
        self.receive_cleanup_arc(Arc::new(error), phase)
    }
    pub(super) fn receive_cleanup_arc(&self, error: SharedError, phase: Phase) -> SharedError {
        self.cleanup_failures.receive(error, phase)
    }
    pub(super) fn cleanup_failures(&self) -> impl Iterator<Item = SharedError> + '_ {
        self.cleanup_failures.failures()
    }
}
