//! The exact supervisor handshake, actual pending Child, and terminal report.
use super::*;
use crate::{
    macos_privilege::{
        command::{inline::Storage, receive::Receiving},
        session::MacosHelperOwner,
    },
    packet_memory::PacketReturnTotals,
};
use std::{
    os::fd::{AsRawFd, FromRawFd, OwnedFd},
    sync::Arc,
    time::{Duration, Instant},
};

fn poll(supervisor: &super::super::super::Supervisor, deadline: Instant) {
    let mut descriptors = [
        libc::pollfd {
            fd: supervisor.poll_fd().unwrap_or(-1),
            events: supervisor.poll_events(),
            revents: 0,
        },
        libc::pollfd {
            fd: supervisor.event_fd().unwrap_or(-1),
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    let left = deadline.saturating_duration_since(Instant::now());
    assert!(
        !left.is_zero(),
        "actual matching process handshake did not progress"
    );
    let ms = left.as_millis().min(i32::MAX as u128) as i32;
    assert!(unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, ms.max(1)) } > 0);
}
#[test]
fn remote_cancel_after_actual_pending_keeps_original_note_exit_and_all_finish_outcomes() {
    actual_cancel(false);
}
#[test]
fn remote_stop_during_real_pre_exec_wait_does_not_wait_for_ack_to_request_cancel() {
    actual_cancel(true);
}
fn actual_cancel(before_ack: bool) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let owner = MacosHelperOwner::prepare(&memory, 131072, false, 65536).unwrap();
    let _context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let mut supervisor = super::super::super::Supervisor::from_source(mode.control().clone());
    supervisor.prepare(701).unwrap();
    supervisor.begin_source(702, &mode).unwrap();
    let raw = unsafe { libc::dup(supervisor.remote_fd().unwrap()) };
    assert!(raw >= 0);
    let remote = unsafe { OwnedFd::from_raw_fd(raw) };
    let (mut helper_output, source_output) = UnixStream::pair().unwrap();
    source_output.set_nonblocking(true).unwrap();
    let storage = std::array::from_fn(|_| {
        Storage::from_prepaid(
            memory
                .reserve_driver_metadata(Storage::layout().unwrap())
                .unwrap(),
        )
        .unwrap()
    });
    let mut receiving = Receiving::new(702, &storage).unwrap();
    let (entered, pending) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        let mut child = ChildOriginal::new();
        if !before_ack {
            child.pending = Some(entered);
        }
        let result = run_fixed_streamed(
            &mut child,
            "/bin/sleep",
            &["60".into()],
            None,
            (remote.as_raw_fd(), 702),
            &mut helper_output,
            131072,
        );
        assert!(child.reaped());
        assert!(child.reusable());
        assert!(child
            .pipes
            .iter()
            .chain(&child.child_pipes)
            .chain(&child.preparing)
            .chain(&child.relocated)
            .flatten()
            .all(Original::closed));
        result.unwrap_err()
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    if before_ack {
        // The controlled child's actual pre_exec report is available, while
        // its read is waiting for the still-unissued NOTE_EXIT ACK.
        poll(&supervisor, deadline);
        assert_eq!(supervisor.read, 0);
        assert!(supervisor.event_fd().is_none());
        owner.stop();
        assert!(
            !mode.control().remote_pending_write(),
            "cancel cannot precede registration ACK"
        );
    }
    while supervisor.live_read != supervisor.live.len() {
        supervisor.advance().unwrap();
        if supervisor.live_read != supervisor.live.len() {
            poll(&supervisor, deadline);
        }
    }
    // NOTE_EXIT was registered before ACK; LIVE is from the same pre_exec.
    assert!(supervisor.event_fd().is_some());
    if !before_ack {
        assert!(!supervisor.completed().unwrap());
        pending.recv_timeout(Duration::from_secs(5)).unwrap();
        owner.stop();
    }
    let error = worker.join().unwrap();
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
    assert!(receiving
        .progress(source_output.as_raw_fd(), &mut [0; 4096], &memory, 131072)
        .unwrap());
    let done = receiving.finished().unwrap();
    assert_eq!(done.operation_errno, libc::EINTR);
    assert_eq!(done.wait_errno, 0);
    assert_eq!(done.cancel_errno, 0);
    assert_eq!(done.close_errnos, [0; 10]);
    assert!(done.child_pid > 0);
    while !supervisor
        .completed_report(702, done.child_pid, done.operation_errno)
        .unwrap()
    {
        poll(&supervisor, deadline);
    }
    assert_eq!(receiving.take().unwrap_err().kind(), io::ErrorKind::Other);
    supervisor.close().unwrap();
    assert!(supervisor.closed());
    assert!(owner.enter().is_err());
}

#[path = "tests/stdio.rs"]
mod stdio;
