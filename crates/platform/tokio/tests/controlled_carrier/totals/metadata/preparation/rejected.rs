//! Full original N returns the same inert controller outside owner/Root locks.
use super::*;
use crate::contract::{
    lifecycle::{CloseReport, DynChannelControl},
    BoxFuture, CloseReason,
};
use crate::controlled_datagram::metadata_pool::MetadataPoolGrants;
use std::sync::atomic::{AtomicBool, Ordering};
struct Probe {
    grants: MetadataPoolGrants,
    dropped: Arc<AtomicBool>,
}
impl DynChannelControl for Probe {
    fn cancel(&self, _: CloseReason) {
        panic!("refused original must remain inert");
    }
    fn wait_closed(&mut self, _: Instant) -> BoxFuture<'_, Result<CloseReport, ChannelError>> {
        panic!("refused original never starts a cleanup operation")
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        // Lock the actual original owner, proving rejection did not drop its
        // caller's arbitrary control destructor under that same owner lock.
        assert_eq!(self.grants.retained(), Some(1));
        self.dropped.store(true, Ordering::Release);
    }
}
pub(super) fn check(owner: &MetadataPoolOwner, tasks: &crate::contract::scope::TaskRegistrar) {
    let dropped = Arc::new(AtomicBool::new(false));
    let control: crate::contract::lifecycle::BoxChannelControl = Box::new(Probe {
        grants: owner.grants(),
        dropped: dropped.clone(),
    })
    .into();
    let pointer = std::ptr::from_ref(control.as_ref());
    let rejected = owner
        .grants()
        .prepare_protocol_original(tasks, control)
        .err()
        .expect("same N is full");
    assert_eq!(rejected.error.reason, Reason::Capacity);
    assert!(std::ptr::eq(
        pointer,
        std::ptr::from_ref(rejected.control.as_ref())
    ));
    assert!(!dropped.load(Ordering::Acquire));
    drop(rejected);
    assert!(dropped.load(Ordering::Acquire));
}
