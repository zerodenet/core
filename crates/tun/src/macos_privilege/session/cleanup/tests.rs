use super::*;
use crate::packet_memory::PacketReturnTotals;
use std::{os::fd::IntoRawFd, sync::Arc};
fn original(dedicated: bool) -> (MacosHelperOwner, crate::packet_memory::NativeReadGrant) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner = MacosHelperOwner::prepare(
        &read.memory_view().unwrap(),
        65536,
        dedicated,
        rpc::CAPACITY,
    )
    .unwrap();
    (owner, read)
}
#[test]
fn non_dedicated_route_duty_retires_its_actual_epoch_zero_control_without_an_initial_actor() {
    let (owner, _read) = original(false);
    let lease = owner.lease_routes().unwrap();
    let (control, mut peer) = UnixStream::pair().unwrap();
    {
        let mut epoch = owner.0.epochs[0].lock().unwrap();
        epoch.native.control = Some(Original::new(control.into_raw_fd(), epoch.id));
    }
    assert!(owner.require_closed().is_err());
    owner.stop();
    lease.finish_cleanup().unwrap();
    assert!(owner.0.initial_driver_returned.load(Ordering::Acquire));
    assert_eq!(owner.0.current.load(Ordering::Acquire), 0);
    owner.require_closed().unwrap();
    use std::io::Read;
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    assert!(owner.0.epochs[0]
        .lock()
        .unwrap()
        .native
        .control
        .as_ref()
        .unwrap()
        .closed());
}
#[test]
fn initial_actor_completion_does_not_claim_or_wait_for_the_sources_actual_live_route_file() {
    let (owner, _read) = original(true);
    let directory = tempfile::tempdir().unwrap();
    let mut route = owner
        .acquire_route_lock(&directory.path().join("route.lock"), false)
        .unwrap();
    owner.stop();
    // No helper was acquired; this real actor must finish its own originals.
    // The acquired route lock belongs to the independent route duty.
    owner.wait_initial().unwrap();
    assert!(owner.0.epochs[0].lock().unwrap().require_closed().is_ok());
    assert!(!route.is_closed());
    assert!(owner.require_closed().is_err());
    route.close_retained().unwrap();
    owner.require_closed().unwrap();
}
