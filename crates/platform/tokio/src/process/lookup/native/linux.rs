use super::*;
use crate::process::platform::Originals;
pub(in crate::process) struct Native {
    originals: Mutex<Originals>,
}
impl Native {
    pub(in crate::process) fn new() -> Self {
        Self {
            originals: Mutex::new(Originals::default()),
        }
    }
    pub(in crate::process) fn originals(&self) -> std::sync::MutexGuard<'_, Originals> {
        self.originals
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(in crate::process) fn confirmed(&self) -> bool {
        match self.originals.try_lock() {
            Ok(originals) => originals.confirmed(),
            Err(std::sync::TryLockError::Poisoned(source)) => source.into_inner().confirmed(),
            Err(std::sync::TryLockError::WouldBlock) => false,
        }
    }
    pub(in crate::process) fn failure(&self) -> Option<Arc<ChannelError>> {
        match self.originals.try_lock() {
            Ok(originals) => originals.failure(),
            Err(std::sync::TryLockError::Poisoned(source)) => source.into_inner().failure(),
            Err(std::sync::TryLockError::WouldBlock) => None,
        }
    }
    /// Caller must already match this lookup's actual joined worker. No result
    /// message, cancellation, start notification or body Done permits this.
    pub(in crate::process) fn collect_after_join(&self) -> Result<(), Arc<ChannelError>> {
        match self.originals.try_lock() {
            Ok(mut originals) => originals.close_all(),
            Err(std::sync::TryLockError::Poisoned(source)) => source.into_inner().close_all(),
            Err(std::sync::TryLockError::WouldBlock) => Err(Arc::new(ChannelError::new(
                Stage::Close,
                Reason::Io,
                Impact::Component,
            ))),
        }
    }
}
