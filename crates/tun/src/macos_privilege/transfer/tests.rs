use super::*;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd};
mod continuation;

fn send(socket: &UnixStream, descriptors: &[RawFd], marker: u8) {
    let mut payload = [marker];
    let mut iov = libc::iovec {
        iov_base: payload.as_mut_ptr().cast(),
        iov_len: 1,
    };
    let bytes = std::mem::size_of_val(descriptors);
    let space = unsafe { libc::CMSG_SPACE(bytes as u32) } as usize;
    let mut aligned = [0_usize; 8];
    assert!(space <= std::mem::size_of_val(&aligned));
    let mut message: libc::msghdr = unsafe { std::mem::zeroed() };
    message.msg_iov = &mut iov;
    message.msg_iovlen = 1;
    message.msg_control = aligned.as_mut_ptr().cast();
    message.msg_controllen = space as _;
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(bytes as u32) as _;
        std::ptr::copy_nonoverlapping(
            descriptors.as_ptr().cast::<u8>(),
            libc::CMSG_DATA(header),
            bytes,
        );
        assert_eq!(libc::sendmsg(socket.as_raw_fd(), &message, 0), 1);
    }
}

#[test]
fn parent_custody_precedes_helper_close_and_local_original_stays_distinct() {
    let (mut parent, mut helper) = UnixStream::pair().unwrap();
    let (native, mut peer) = UnixStream::pair().unwrap();
    let mut original = Original::new(native.into_raw_fd(), 31);
    let transfer = Transfer::new(31);
    transfer.submit_create();
    send(&helper, &[original.fd()], 1);
    let fd = transfer.receive_fd(parent.as_raw_fd()).unwrap();
    // This is the actual owned parent descriptor, installed before ACK.
    let owned = unsafe { OwnedFd::from_raw_fd(fd) };
    assert!(transfer.confirm(&mut parent, fd).is_err());
    assert!(original.usable());
    transfer.claim_received(owned.as_raw_fd()).unwrap();
    let worker = std::thread::spawn(move || {
        original.finish(&mut helper).unwrap();
        assert!(original.closed());
    });
    transfer.confirm(&mut parent, owned.as_raw_fd()).unwrap();
    worker.join().unwrap();
    transfer.require_closed().unwrap();
    // Helper close does not close the independent parent original.
    let mut view = UnixStream::from(owned);
    view.write_all(b"live").unwrap();
    let mut live = [0; 4];
    peer.read_exact(&mut live).unwrap();
    assert_eq!(&live, b"live");
    let mut local = Original::new(view.into_raw_fd(), 31);
    local.close_once().unwrap();
    assert!(local.closed());
    assert_eq!(peer.read(&mut live).unwrap(), 0);
}

#[test]
fn malformed_ancillary_keeps_delivered_original_until_explicit_close() {
    let (parent, helper) = UnixStream::pair().unwrap();
    let (native, mut peer) = UnixStream::pair().unwrap();
    let transfer = Transfer::new(32);
    transfer.submit_create();
    send(&helper, &[native.as_raw_fd()], 7);
    assert!(transfer.receive_fd(parent.as_raw_fd()).is_err());
    drop(native);
    transfer.observe_remote_settled();
    assert!(transfer.require_closed().is_err());
    transfer.close_unclaimed().unwrap();
    transfer.require_closed().unwrap();
    assert_eq!(peer.read(&mut [0]).unwrap(), 0);
}

#[test]
fn wrong_parent_nonce_never_closes_original_or_becomes_a_receipt() {
    let (mut parent, mut helper) = UnixStream::pair().unwrap();
    let (native, mut peer) = UnixStream::pair().unwrap();
    let mut original = Original::new(native.into_raw_fd(), 33);
    parent.write_all(&34_u64.to_be_bytes()).unwrap();
    assert!(original.finish(&mut helper).is_err());
    assert!(original.usable());
    let mut borrowed =
        unsafe { std::mem::ManuallyDrop::new(UnixStream::from_raw_fd(original.fd())) };
    borrowed.write_all(b"x").unwrap();
    peer.read_exact(&mut [0]).unwrap();
    original.close_once().unwrap();
}
