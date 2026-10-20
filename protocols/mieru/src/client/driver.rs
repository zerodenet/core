//! Legacy workers retain their behavior; native workers belong to the real scope.
#[cfg(feature = "runtime")]
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

pub(super) enum DriverHandle {
    Legacy(tokio::task::JoinHandle<()>),
    #[cfg(feature = "runtime")]
    Managed(Arc<ManagedDriver>),
}
impl DriverHandle {
    pub(super) fn cancel(&self) {
        match self {
            Self::Legacy(task) => task.abort(),
            #[cfg(feature = "runtime")]
            Self::Managed(driver) => driver.cancel.cancel(),
        }
    }
    pub(super) fn is_finished(&self) -> bool {
        match self {
            Self::Legacy(task) => task.is_finished(),
            #[cfg(feature = "runtime")]
            Self::Managed(driver) => driver.finished.load(Ordering::Acquire),
        }
    }
}
#[cfg(feature = "runtime")]
pub(crate) struct ManagedDriver {
    pub(crate) cancel: zero_platform_tokio::contract::CancellationToken,
    finished: AtomicBool,
}
#[cfg(feature = "runtime")]
impl ManagedDriver {
    pub(crate) fn new(cancel: zero_platform_tokio::contract::CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            cancel,
            finished: AtomicBool::new(false),
        })
    }
    pub(crate) fn completion(self: &Arc<Self>) -> Completion {
        Completion(self.clone())
    }
}
#[cfg(feature = "runtime")]
pub(crate) struct Completion(Arc<ManagedDriver>);
#[cfg(feature = "runtime")]
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.finished.store(true, Ordering::Release);
    }
}
