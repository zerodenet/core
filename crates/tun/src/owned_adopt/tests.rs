use super::*;
use std::os::fd::AsRawFd;

#[test]
fn cold_owner_is_constructible_without_runtime_or_native_acquisition() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let owner = AdoptedTunOwner::new();
    assert!(matches!(
        owner.outcome(),
        TunAcquisitionOutcome::KnownNoNative
    ));
    drop(owner);
}

#[test]
fn failed_native_duplicate_is_no_acquire_and_retains_the_same_actual_os_error() {
    let owner = AdoptedTunOwner::new();
    let first = match owner.acquire(i32::MAX, "not-acquired") {
        Ok(_) => panic!("invalid original descriptor"),
        Err(error) => error,
    };
    assert!(matches!(
        first.outcome,
        TunAcquisitionOutcome::KnownNoNative
    ));
    assert_eq!(first.source.raw_os_error(), Some(libc::EBADF));
    let retry = match owner.acquire(i32::MAX, "not-acquired") {
        Ok(_) => panic!("one attempted generation"),
        Err(error) => error,
    };
    assert!(Arc::ptr_eq(&first.source, &retry.source));
}

#[test]
fn readonly_backend_validation_failure_explicitly_closes_the_original_duplicate_without_runtime() {
    let (host, peer) = std::os::unix::net::UnixDatagram::pair().unwrap();
    host.set_nonblocking(true).unwrap();
    let expected = Backend::validate_adopt(host.as_raw_fd(), "not-a-tun").unwrap_err();
    let owner = AdoptedTunOwner::new();
    let error = match owner.acquire(host.as_raw_fd(), "not-a-tun") {
        Ok(_) => panic!("socket is not a TUN"),
        Err(error) => error,
    };
    assert_eq!(error.source.kind(), expected.kind());
    assert_eq!(error.source.raw_os_error(), expected.raw_os_error());
    assert_eq!(error.source.to_string(), expected.to_string());
    assert!(Arc::ptr_eq(
        &owner.acquisition_failure().unwrap(),
        &error.source
    ));
    let TunAcquisitionOutcome::Closed(closed) = error.outcome else {
        panic!("explicit close required after real successful duplicate")
    };
    let retained = owner.close().unwrap();
    assert!(Arc::ptr_eq(&closed, &retained));
    assert!(closed.identity().same_owner(retained.identity()));
    host.send(b"original host descriptor remains open").unwrap();
    let mut data = [0; 64];
    let size = peer.recv(&mut data).unwrap();
    assert_eq!(&data[..size], b"original host descriptor remains open");
}

#[cfg(target_os = "linux")]
#[tokio::test]
#[ignore = "requires /dev/net/tun and CAP_NET_ADMIN"]
async fn actual_adopted_tun_view_drop_does_not_release_the_persistent_original() {
    use crate::TunDevice;
    let original = crate::LinuxTun::create(None).unwrap();
    let owner = AdoptedTunOwner::new();
    let view = owner
        .acquire(original.as_raw_fd(), original.name())
        .unwrap();
    let identity = view.owned_identity().clone();
    drop(view);
    let TunAcquisitionOutcome::Unknown(retained) = owner.outcome() else {
        panic!("view Drop cannot issue a close receipt")
    };
    assert!(retained.unwrap().same_owner(&identity));
    let receipt = owner.close().unwrap();
    assert!(receipt.identity().same_owner(&identity));
    assert!(Arc::ptr_eq(&receipt, &owner.close().unwrap()));
    assert_ne!(
        unsafe { libc::fcntl(original.as_raw_fd(), libc::F_GETFD) },
        -1
    );
}
