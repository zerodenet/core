use super::*;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

#[tokio::test]
async fn explicit_close_deregisters_and_closes_only_the_owned_duplicate() {
    let (original, _peer) = std::os::unix::net::UnixDatagram::pair().unwrap();
    original.set_nonblocking(true).unwrap();
    // Same duplication operation as adopt(), without requiring privileged TUN.
    let duplicate = unsafe { libc::fcntl(original.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
    assert!(duplicate >= 3);
    let duplicate = unsafe { OwnedFd::from_raw_fd(duplicate) };
    let identity = OwnedTunIdentity::from_fd(duplicate.as_raw_fd());
    let same_number_different_owner = OwnedTunIdentity::from_fd(duplicate.as_raw_fd());
    let registered = unsafe { tokio::io::unix::AsyncFd::register(duplicate) }.unwrap();
    let receipt = close_fd(
        registered.into_inner(),
        "actual-owned-duplicate".into(),
        identity.clone(),
    )
    .unwrap();
    assert_eq!(receipt.interface(), "actual-owned-duplicate");
    assert!(receipt.identity().same_owner(&identity));
    assert!(!receipt.identity().same_owner(&same_number_different_owner));
    assert_ne!(
        unsafe { libc::fcntl(original.as_raw_fd(), libc::F_GETFD) },
        -1
    );
    original
        .send(b"host original remains owned and usable")
        .unwrap();
}
