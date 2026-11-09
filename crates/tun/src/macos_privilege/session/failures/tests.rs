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
