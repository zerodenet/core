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
    #[cfg(feature = "runtime")]
    pub(super) fn native_witness(
        &self,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoinWitness> {
        match self {
            Self::Legacy(_) => None,
            Self::Managed(driver) => driver.witness.lock().unwrap().clone(),
        }
    }
    #[cfg(feature = "runtime")]
    pub(super) fn native_origin(
        &self,
    ) -> Option<zero_platform_tokio::controlled_carrier::CarrierOrigin> {
        match self {
            Self::Legacy(_) => None,
            Self::Managed(driver) => driver.original.lock().unwrap().clone(),
        }
    }
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
    witness: std::sync::Mutex<Option<zero_platform_tokio::contract::scope::TaskJoinWitness>>,
    original: std::sync::Mutex<Option<zero_platform_tokio::controlled_carrier::CarrierOrigin>>,
}
#[cfg(feature = "runtime")]
impl ManagedDriver {
    pub(crate) fn new(cancel: zero_platform_tokio::contract::CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            cancel,
            finished: AtomicBool::new(false),
            witness: std::sync::Mutex::new(None),
            original: std::sync::Mutex::new(None),
        })
    }
    pub(crate) fn attach_original(
        &self,
        adoption: &zero_platform_tokio::controlled_carrier::CarrierAdoption,
    ) -> Result<(), zero_platform_tokio::contract::ChannelError> {
        let witness = self
            .witness
            .lock()
            .unwrap()
            .clone()
            .expect("registered original driver");
        let original = adoption.origin(&witness)?;
        *self.original.lock().unwrap() = Some(original);
        Ok(())
    }
    pub(crate) fn completion(self: &Arc<Self>) -> Completion {
        Completion(self.clone())
    }
    pub(crate) fn registered(
        &self,
        witness: zero_platform_tokio::contract::scope::TaskJoinWitness,
    ) {
        *self.witness.lock().unwrap() = Some(witness);
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
