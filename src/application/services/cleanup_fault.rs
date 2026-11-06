//! Bounded receivers for the actual application owners; no resource authority.
use super::*;
type OriginalError = Arc<dyn Error + Send + Sync>;
#[derive(Clone, Copy)]
pub(super) enum CleanupOwner {
    Hooks,
    PreparedHooks,
    #[cfg(feature = "event-dispatcher")]
    Monitor,
    #[cfg(feature = "event-dispatcher")]
    DispatcherActivation,
    #[cfg(feature = "event-dispatcher")]
    DispatcherPending,
    #[cfg(feature = "event-dispatcher")]
    DispatcherClosed,
    #[cfg(feature = "event-dispatcher")]
    NetworkPending,
    #[cfg(feature = "event-dispatcher")]
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
    #[cfg(feature = "event-dispatcher")]
    pub(super) fn remember_reported_dispatcher_failure(&mut self, source: OriginalError) {
        // This exact readiness failure has already been returned, and its
        // actual handle separately confirmed every original closed. Keep its
        // complete typed archive in the existing current-owner receiver. A
        // later closed generation replaces this diagnostic; it is no history
        // list and does not erase an independent unresolved cleanup fence.
        self.cleanup_receivers.0[CleanupOwner::DispatcherClosed as usize] = Some(source);
    }
    #[cfg(test)]
    pub(super) fn cleanup_receiver(&self, owner: CleanupOwner) -> Option<&OriginalError> {
        self.cleanup_receivers.0[owner as usize].as_ref()
    }
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
