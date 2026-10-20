//! One in-progress host phase tracks only its actual candidate instances.
use super::*;
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Weak,
};

#[derive(Default)]
pub(crate) struct PreparingInbounds {
    closing: AtomicBool,
    owners: Mutex<BTreeMap<ComponentInstanceId, Weak<InboundOwner>>>,
}
impl PreparingInbounds {
    pub(crate) fn register(&self, owner: &Arc<InboundOwner>) -> Result<(), EngineError> {
        let mut owners = self.owners.lock().expect("preparing listeners poisoned");
        // Metadata contains only current, unready actual owners; never a
        // history of configurations, completed instances or resource handles.
        owners.retain(|_, owner| {
            owner
                .upgrade()
                .is_some_and(|owner| !owner.is_ready() && owner.terminal().is_none())
        });
        owners.insert(owner.identity(), Arc::downgrade(owner));
        if self.closing.load(Ordering::Acquire) {
            owner.task_access().request_stop();
            return Err(owner.ready_stopped());
        }
        Ok(())
    }
    pub(crate) fn begin_shutdown(&self) {
        let owners = self.owners.lock().expect("preparing listeners poisoned");
        // The same short registration lock fences a late synchronous install.
        self.closing.store(true, Ordering::Release);
        for (identity, owner) in owners.iter() {
            if let Some(owner) = owner.upgrade() {
                if owner.identity() == *identity && !owner.is_ready() {
                    owner.task_access().request_stop();
                }
            }
        }
    }
    pub(crate) fn ensure_open(&self) -> Result<(), EngineError> {
        if self.closing.load(Ordering::Acquire) {
            Err(std::io::Error::new(std::io::ErrorKind::Interrupted, PreparationStopped).into())
        } else {
            Ok(())
        }
    }
}
#[derive(Debug)]
struct PreparationStopped;
impl std::fmt::Display for PreparationStopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("host stopped this actual inbound preparation phase")
    }
}
impl std::error::Error for PreparationStopped {}
pub(crate) fn preparation_stopped(error: &EngineError) -> bool {
    let EngineError::Io(error) = error else {
        return false;
    };
    error
        .get_ref()
        .is_some_and(|source| source.is::<PreparationStopped>())
        || entry::readiness_stopped(error)
}
#[cfg(test)]
mod tests;
