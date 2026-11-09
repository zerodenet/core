use super::*;

#[test]
fn partial_parent_ack_continues_same_original_without_restarting_nonce() {
    let (mut parent, mut helper) = UnixStream::pair().unwrap();
    helper.set_nonblocking(true).unwrap();
    let (native, mut peer) = UnixStream::pair().unwrap();
    let mut original = Original::new(native.into_raw_fd(), 0x0102030405060708);
    let nonce = 0x0102030405060708_u64.to_be_bytes();
    parent.write_all(&nonce[..3]).unwrap();
    assert_eq!(
        original.finish(&mut helper).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(original.usable());
    parent.write_all(&nonce[3..]).unwrap();
    original.finish(&mut helper).unwrap();
    assert!(original.closed());
    let mut reply = [0; 12];
    parent.read_exact(&mut reply).unwrap();
    assert_eq!(&reply[..8], &nonce);
    assert_eq!(&reply[8..], &[0; 4]);
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
    // Re-observation sends and closes nothing again.
    original.finish(&mut helper).unwrap();
    parent.set_nonblocking(true).unwrap();
    assert_eq!(
        parent.read(&mut reply).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
}

#[test]
fn outcome_observation_does_not_take_the_blocking_confirmation_lock() {
    let (mut parent, mut helper) = UnixStream::pair().unwrap();
    let (native, _peer) = UnixStream::pair().unwrap();
    let transfer = Arc::new(Transfer::new(35));
    transfer.submit_create();
    send(&helper, &[native.as_raw_fd()], 1);
    let fd = transfer.receive_fd(parent.as_raw_fd()).unwrap();
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    transfer.claim_received(fd).unwrap();
    let task = transfer.clone();
    let worker = std::thread::spawn(move || task.confirm(&mut parent, fd));
    let mut ack = [0; 8];
    helper.read_exact(&mut ack).unwrap();
    assert_eq!(u64::from_be_bytes(ack), 35);
    // Confirmation is now at a real IPC read. The pure observer uses neither
    // that stream nor the continuation lock, so it cannot wait on the peer.
    assert!(transfer.require_closed().is_err());
    assert!(!transfer.remote_closed());
    helper.write_all(&ack).unwrap();
    helper.write_all(&0_i32.to_be_bytes()).unwrap();
    worker.join().unwrap().unwrap();
    transfer.require_closed().unwrap();
    drop(owned);
}

#[test]
fn close_failure_is_stable_and_never_retries_a_possibly_reused_descriptor() {
    let mut original = Original::new(-1, 36);
    assert_eq!(
        original.close_once().unwrap_err().raw_os_error(),
        Some(libc::EBADF)
    );
    assert!(!original.closed());
    assert!(!original.usable());
    assert_eq!(
        original.close_once().unwrap_err().raw_os_error(),
        Some(libc::EBADF)
    );
}
