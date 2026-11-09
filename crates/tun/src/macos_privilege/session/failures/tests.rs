use super::*;
use std::sync::Arc;
#[test]
fn a_failed_file_operation_and_its_independent_failed_close_keep_both_original_typed_sources() {
    let root = Arc::new(crate::packet_memory::PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, rpc::CAPACITY)
            .unwrap();
    let operation = Arc::new(io::Error::from_raw_os_error(libc::EIO));
    let close = Arc::new(io::Error::from_raw_os_error(libc::EBADF));
    owner.0.failures.retain(
        0,
        Phase::RouteTransientOperation,
        io::Error::other(operation.clone()),
    );
    owner
        .0
        .failures
        .retain(0, Phase::RouteTransient, io::Error::other(close.clone()));
    let original = owner.failure().unwrap();
    let returned = retained(original.clone());
    let aggregate = returned.get_ref().unwrap().source().unwrap();
    assert!(std::ptr::eq(aggregate, original.as_error()));
    let payload = aggregate.downcast_ref::<Failures>().unwrap();
    assert!(Arc::ptr_eq(
        payload.slots[28]
            .get()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &operation
    ));
    assert!(Arc::ptr_eq(
        payload.slots[24]
            .get()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &close
    ));
    // The other actual epoch has independent slots; its close cannot overwrite
    // the first operation or inherit a previous phase's accepted/source bit.
    owner.0.failures.retain(
        1,
        Phase::RouteTransient,
        io::Error::from_raw_os_error(libc::EINTR),
    );
    assert_eq!(
        payload.slots[25].get().unwrap().raw_os_error(),
        Some(libc::EINTR)
    );
    assert!(Arc::ptr_eq(
        payload.slots[24]
            .get()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &close
    ));
}

#[test]
fn same_source_late_cleanup_feedback_preserves_original_cause_without_a_paid_payload_self_cycle() {
    use std::{io::Read, os::fd::IntoRawFd};
    let root = Arc::new(crate::packet_memory::PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, rpc::CAPACITY)
            .unwrap();
    let lease = owner.lease_routes().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let mut route = owner
        .acquire_route_lock(&directory.path().join("route.lock"), false)
        .unwrap();
    let (control, mut peer) = UnixStream::pair().unwrap();
    {
        let mut epoch = owner.0.epochs[0].lock().unwrap();
        epoch.native.control = Some(Original::new(control.into_raw_fd(), epoch.id));
    }
    let original = Arc::new(std::fs::read(directory.path().join("missing-original")).unwrap_err());
    owner
        .0
        .retain(0, Phase::ToolProgress, io::Error::other(original.clone()));
    let weak = PacketMetadataArc::downgrade(&owner.0.failures);
    // Same production close method returns its already accepted failure while
    // another real Source original is still live. Replay the actual retry arm.
    let delivery = owner.finish_registered_epoch(0).unwrap_err();
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    assert!(owner.require_closed().is_err());
    owner.0.retain(0, Phase::Shutdown, delivery);
    assert!(owner.0.failures.slots[8].get().is_none());
    assert!(Arc::ptr_eq(
        owner.0.failures.slots[6]
            .get()
            .unwrap()
            .get_ref()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &original
    ));
    owner.stop();
    route.close_retained().unwrap();
    let delivery = lease.finish_cleanup().unwrap_err();
    owner.0.retain(0, Phase::Shutdown, delivery);
    owner.require_closed().unwrap();
    drop(route);
    drop(lease);
    drop(owner);
    assert!(
        weak.upgrade().is_none(),
        "actual retired Source still retained itself through feedback"
    );
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes());
    drop(weak); // real last weak/header release, not a body-completion refund
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn a_foreign_paid_payload_is_retained_as_an_actual_original_instead_of_being_discarded_as_feedback()
{
    let root = Arc::new(crate::packet_memory::PacketReturnTotals::default());
    let baseline = root.driver_metadata().occupied;
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let owner = MacosHelperOwner::prepare(&memory, 65536, false, rpc::CAPACITY).unwrap();
    let foreign = MacosHelperOwner::prepare(&memory, 65536, false, rpc::CAPACITY).unwrap();
    foreign.0.retain(
        0,
        Phase::ToolProgress,
        io::Error::from_raw_os_error(libc::EIO),
    );
    let weak = PacketMetadataArc::downgrade(&foreign.0.failures);
    let source = foreign.failure().unwrap();
    owner.0.retain(0, Phase::Shutdown, retained(source.clone()));
    let stored = owner.0.failures.slots[8]
        .get()
        .unwrap()
        .get_ref()
        .unwrap()
        .downcast_ref::<Retained>()
        .unwrap();
    assert!(stored.0.ptr_eq(&source));
    drop(source);
    drop(foreign);
    assert!(weak.upgrade().is_some());
    drop(owner);
    assert!(weak.upgrade().is_none());
    drop(weak);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
