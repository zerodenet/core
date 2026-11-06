//! Bounded receivers for the actual application owners; no resource authority.
use super::*;
type OriginalError = Arc<dyn Error + Send + Sync>;
#[derive(Clone, Copy)]
pub(super) enum CleanupOwner {
    Hooks,
    PreparedHooks,
    Monitor,
    DispatcherActivation,
    DispatcherPending,
    DispatcherClosed,
    NetworkPending,
    NetworkClosed,
}
#[derive(Clone, Default)]
pub(super) struct CleanupReceivers([Option<OriginalError>; 8]);
struct ApplicationCleanupFailure {
    first: OriginalError,
    _receivers: CleanupReceivers,
}
impl std::fmt::Debug for ApplicationCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ApplicationCleanupFailure")
            .finish_non_exhaustive()
    }
}
impl std::fmt::Display for ApplicationCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self.first.as_ref(), f)
    }
}
impl Error for ApplicationCleanupFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(self.first.as_ref())
    }
}
impl ApplicationServiceState {
    pub(super) fn remember_cleanup_failure(&mut self, owner: CleanupOwner, source: OriginalError) {
        self.cleanup_fault.get_or_insert_with(|| source.clone());
        // No new owner is admitted after this fence. Pending and physically
        // closed reports have separate fixed receivers: the latter keeps the
        // complete original owner archive before its actual slot is cleared.
        self.cleanup_receivers.0[owner as usize].get_or_insert(source);
    }
    pub(super) fn cleanup_error(&self) -> Option<OriginalError> {
        self.cleanup_fault.as_ref().map(|first| {
            Arc::new(ApplicationCleanupFailure {
                first: first.clone(),
                _receivers: self.cleanup_receivers.clone(),
            }) as OriginalError
        })
    }
}
