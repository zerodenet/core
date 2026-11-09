//! Exact original registration settlement, independent of native closure.
use super::sources::memory::Allocation;
use super::*;
use std::sync::atomic::{AtomicBool, Ordering};
#[derive(Default)]
pub(super) struct State {
    native: OnceLock<bool>,
    returned: AtomicBool,
    root_returned: AtomicBool,
    changed: tokio::sync::Notify,
}
#[derive(Clone)]
pub(super) struct Completion(Allocation<State>);
impl Completion {
    pub(super) fn new(state: Allocation<State>) -> Self {
        Self(state)
    }
    pub(super) fn get(&self) -> Option<&bool> {
        self.0.native.get()
    }
    pub(super) fn set(&self, acquired: bool) -> Result<(), bool> {
        self.0.native.set(acquired)
    }
    pub(super) fn same(&self, other: &Self) -> bool {
        self.0.same(&other.0)
    }
    pub(super) fn publish_registration_return(&self) {
        assert!(
            self.get().is_some(),
            "registration return follows actual native settlement"
        );
        self.0.returned.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
    pub(super) fn publish_root_return(&self) {
        assert!(
            self.0.returned.load(Ordering::Acquire),
            "global return follows actual local original ACK settlement"
        );
        self.0.root_returned.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
    pub(super) fn observation(
        &self,
        instance: ComponentInstanceId,
        ordinal: u64,
    ) -> MetadataPoolRegistrationObservation {
        MetadataPoolRegistrationObservation {
            completion: self.clone(),
            instance,
            ordinal,
        }
    }
}
/// Read-only strong custody of this original's pure completion cell. No Slot,
/// Scope, IO, ACK or budget authority can be reconstructed from this handle.
#[derive(Clone)]
pub struct MetadataPoolRegistrationObservation {
    completion: Completion,
    instance: ComponentInstanceId,
    ordinal: u64,
}
#[derive(Clone)]
pub struct MetadataPoolRegistrationReleased {
    original: MetadataPoolRegistrationObservation,
}
impl MetadataPoolRegistrationObservation {
    pub fn instance(&self) -> ComponentInstanceId {
        self.instance
    }
    /// Positive publication by the actual charged Root after removing its row.
    /// Native close or disappearance of a weak provider does not set this bit.
    pub fn root_returned(&self) -> bool {
        self.completion.0.root_returned.load(Ordering::Acquire)
    }
    pub fn released(&self) -> Option<MetadataPoolRegistrationReleased> {
        self.completion.0.returned.load(Ordering::Acquire).then(|| {
            MetadataPoolRegistrationReleased {
                original: self.clone(),
            }
        })
    }
    pub async fn wait_original(&self) -> MetadataPoolRegistrationReleased {
        loop {
            let changed = self.completion.0.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(receipt) = self.released() {
                return receipt;
            }
            changed.await;
        }
    }
}
impl MetadataPoolRegistrationReleased {
    pub fn matches(&self, original: &MetadataPoolRegistrationObservation) -> bool {
        self.original.instance == original.instance
            && self.original.ordinal == original.ordinal
            && self.original.completion.same(&original.completion)
            && self.original.completion.0.returned.load(Ordering::Acquire)
    }
}
