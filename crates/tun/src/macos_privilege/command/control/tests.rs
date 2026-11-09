//! Byte-progress invariants, independent of actual NOTE_EXIT authority.
use super::*;
use crate::{macos_privilege::session::MacosHelperOwner, packet_memory::PacketReturnTotals};
use std::{
    io::{Read, Write},
    os::fd::AsRawFd,
    os::unix::net::UnixStream,
    sync::Arc,
};
#[test]
fn partial_previous_cancel_finishes_before_next_cleanup_ack_and_keeps_its_original_nonce() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, 65536).unwrap();
    let lease = owner.lease_routes().unwrap();
    let context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let control = mode.control();
    let (mut local, mut peer) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    control.bind_remote(local.as_raw_fd()).unwrap();
    control.begin_remote(800, &mode).unwrap();
    owner.stop(); // Before registration, no ACK/CANCEL can have been written.
                  // Precisely the metadata state after 4 actual CANCEL bytes were written.
                  // No native/task/registration completion is inferred by this unit test.
    let mut first = [0; MESSAGE];
    first[0] = CANCEL;
    first[1..].copy_from_slice(&800u64.to_be_bytes());
    local.write_all(&first[..4]).unwrap();
    {
        let mut original = control.remote.lock().unwrap();
        original.registered = true;
        original.ack = MESSAGE;
        original.cancel = 4;
    }
    drop(context);
    let context = lease.prepare_cleanup().unwrap();
    let cleanup = owner.test_command_mode().unwrap();
    control.begin_remote(801, &cleanup).unwrap();
    assert!(control.remote.lock().unwrap().previous_cancel.is_some());
    assert!(control.remote_pending_write());
    control.acknowledge_remote(801).unwrap();
    let mut received = [0; MESSAGE * 2];
    peer.read_exact(&mut received).unwrap();
    assert_eq!(&received[..MESSAGE], &first);
    assert_eq!(received[MESSAGE], ACK);
    assert_eq!(&received[MESSAGE + 1..], &801u64.to_be_bytes());
    assert!(!control.remote_pending_write());
    control.unbind_remote().unwrap();
    drop(context);
}

#[test]
fn partial_previous_exit_finishes_before_new_cleanup_ack_without_replaying_or_changing_nonce() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, 65536).unwrap();
    let lease = owner.lease_routes().unwrap();
    let context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let control = mode.control();
    let (mut local, mut peer) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    control.bind_remote(local.as_raw_fd()).unwrap();
    control.begin_remote(820, &mode).unwrap();
    let mut frame = [0; MESSAGE];
    frame[0] = EXIT;
    frame[1..].copy_from_slice(&820u64.to_be_bytes());
    local.write_all(&frame[..4]).unwrap();
    {
        let mut original = control.remote.lock().unwrap();
        original.registered = true;
        original.ack = MESSAGE;
        original.exited = true;
        original.exit = 4;
    }
    drop(context);
    let context = lease.prepare_cleanup().unwrap();
    let cleanup = owner.test_command_mode().unwrap();
    control.begin_remote(821, &cleanup).unwrap();
    assert!(control.remote.lock().unwrap().previous_exit.is_some());
    control.acknowledge_remote(821).unwrap();
    let mut bytes = [0; MESSAGE * 2];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes[..MESSAGE], &frame);
    assert_eq!(bytes[MESSAGE], ACK);
    assert_eq!(&bytes[MESSAGE + 1..], &821u64.to_be_bytes());
    assert!(!control.remote_pending_write());
    control.unbind_remote().unwrap();
    drop(context);
}

#[test]
fn stop_completes_the_same_partial_registered_ack_before_emitting_cancel() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, 65536).unwrap();
    let context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let control = mode.control();
    let (mut local, mut peer) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    control.bind_remote(local.as_raw_fd()).unwrap();
    control.begin_remote(830, &mode).unwrap();
    let mut ack = [0; MESSAGE];
    ack[0] = ACK;
    ack[1..].copy_from_slice(&830u64.to_be_bytes());
    local.write_all(&ack[..4]).unwrap();
    {
        let mut state = control.remote.lock().unwrap();
        state.registered = true;
        state.ack = 4;
    }
    owner.stop();
    let mut bytes = [0; MESSAGE * 2];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(&bytes[..MESSAGE], &ack);
    assert_eq!(bytes[MESSAGE], CANCEL);
    assert_eq!(&bytes[MESSAGE + 1..], &830u64.to_be_bytes());
    assert!(!control.remote_pending_write());
    control.unbind_remote().unwrap();
    drop(context);
}

#[test]
fn same_nonce_partial_exit_then_source_stop_finishes_only_its_original_frame() {
    terminal_stop(false);
}
#[test]
fn complete_exit_then_late_source_stop_writes_nothing_and_keeps_the_original_error() {
    terminal_stop(true);
}
fn terminal_stop(complete: bool) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, 65536).unwrap();
    let context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let control = mode.control();
    let (mut local, mut peer) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    peer.set_nonblocking(true).unwrap();
    control.bind_remote(local.as_raw_fd()).unwrap();
    control.begin_remote(840, &mode).unwrap();
    // Retain an actual earlier native operation cause on this same socket;
    // terminal progress/stop must not replace or discard the fixed fault slot.
    let mut value = 0i32;
    let mut length = std::mem::size_of_val(&value) as libc::socklen_t;
    assert_eq!(
        unsafe {
            libc::getsockopt(
                local.as_raw_fd(),
                -1,
                -1,
                (&mut value as *mut i32).cast(),
                &mut length,
            )
        },
        -1
    );
    control
        .remote_fault
        .set(io::Error::last_os_error())
        .unwrap();
    let original = control.remote_fault.get().unwrap() as *const io::Error;
    let errno = control.remote_fault.get().unwrap().raw_os_error();
    let mut frame = [0; MESSAGE];
    frame[0] = EXIT;
    frame[1..].copy_from_slice(&840u64.to_be_bytes());
    let offset = if complete { MESSAGE } else { 4 };
    local.write_all(&frame[..offset]).unwrap();
    {
        let mut state = control.remote.lock().unwrap();
        state.registered = true;
        state.ack = MESSAGE;
        state.exited = true;
        state.exit = offset;
    }
    if complete {
        drop(peer);
        owner.stop();
    } else {
        owner.stop();
        let mut bytes = [0; MESSAGE];
        peer.read_exact(&mut bytes).unwrap();
        assert_eq!(bytes, frame);
        assert_eq!(
            peer.read(&mut [0]).unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
    }
    assert!(!control.remote_pending_write());
    {
        let state = control.remote.lock().unwrap();
        assert_eq!(state.cancel, 0, "never invent a sent CANCEL");
        assert_eq!(state.exit, MESSAGE);
    }
    assert_eq!(
        control.remote_fault.get().unwrap() as *const io::Error,
        original
    );
    assert_eq!(control.remote_fault.get().unwrap().raw_os_error(), errno);
    control.unbind_remote().unwrap();
    drop(context);
}

#[test]
fn real_terminal_observation_cannot_erase_an_already_partial_cancel_suffix() {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let owner =
        MacosHelperOwner::prepare(&read.memory_view().unwrap(), 65536, false, 65536).unwrap();
    let context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let control = mode.control();
    let (mut local, mut peer) = UnixStream::pair().unwrap();
    local.set_nonblocking(true).unwrap();
    peer.set_nonblocking(true).unwrap();
    control.bind_remote(local.as_raw_fd()).unwrap();
    control.begin_remote(850, &mode).unwrap();
    let mut frame = [0; MESSAGE];
    frame[0] = CANCEL;
    frame[1..].copy_from_slice(&850u64.to_be_bytes());
    local.write_all(&frame[..4]).unwrap();
    {
        let mut state = control.remote.lock().unwrap();
        state.registered = true;
        state.ack = MESSAGE;
        state.cancel = 4;
        state.exited = true;
    }
    owner.stop();
    let mut bytes = [0; MESSAGE];
    peer.read_exact(&mut bytes).unwrap();
    assert_eq!(bytes, frame);
    assert_eq!(
        peer.read(&mut [0]).unwrap_err().kind(),
        io::ErrorKind::WouldBlock
    );
    {
        let state = control.remote.lock().unwrap();
        assert_eq!(state.cancel, MESSAGE);
        assert_eq!(state.exit, 0, "no EXIT bytes were emitted");
    }
    assert!(!control.remote_pending_write());
    control.unbind_remote().unwrap();
    drop(context);
}
