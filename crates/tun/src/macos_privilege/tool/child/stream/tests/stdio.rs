//! All managed stdio has closed; the actual Child still waits on a test FIFO.
use super::*;
use std::{ffi::CString, io::Write, os::unix::ffi::OsStrExt};
#[test]
fn closed_stdio_child_still_pending_can_be_cancelled_by_the_same_source_endpoint() {
    actual(false);
}
#[test]
fn closed_stdio_child_natural_exit_advances_original_parent_event_and_true_helper_wait() {
    actual(true);
}
fn actual(natural: bool) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let memory = read.memory_view().unwrap();
    let owner = MacosHelperOwner::prepare(&memory, 131072, false, 65536).unwrap();
    let context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let mut supervisor =
        super::super::super::super::Supervisor::from_source(mode.control().clone());
    supervisor.prepare(901).unwrap();
    supervisor.begin_source(902, &mode).unwrap();
    let raw = unsafe { libc::dup(supervisor.remote_fd().unwrap()) };
    assert!(raw >= 0);
    let remote = unsafe { OwnedFd::from_raw_fd(raw) };
    let (mut output, source) = UnixStream::pair().unwrap();
    source.set_nonblocking(true).unwrap();
    let storage = std::array::from_fn(|_| {
        Storage::from_prepaid(
            memory
                .reserve_driver_metadata(Storage::layout().unwrap())
                .unwrap(),
        )
        .unwrap()
    });
    let mut receiving = Receiving::new(902, &storage).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("continue");
    let name = CString::new(path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(name.as_ptr(), 0o600) }, 0);
    // Test-only fixed script: no descendant is spawned. The original shell
    // closes its stdio and waits in its own builtin read until this FIFO opens.
    let script = format!("exec 0<&- 1>&- 2>&-; read -r value < '{}'", path.display());
    let (entered, pending) = std::sync::mpsc::sync_channel(0);
    let worker = std::thread::spawn(move || {
        let mut child = ChildOriginal::new();
        child.pending = Some(entered);
        child.pending_after_stdio = true;
        let result = run_fixed_streamed(
            &mut child,
            "/bin/sh",
            &["-c".into(), script],
            None,
            (remote.as_raw_fd(), 902),
            &mut output,
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
        result
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while supervisor.live_read != supervisor.live.len() {
        supervisor.advance().unwrap();
        if supervisor.live_read != supervisor.live.len() {
            poll(&supervisor, deadline);
        }
    }
    pending.recv_timeout(Duration::from_secs(5)).unwrap();
    assert!(
        !supervisor.completed().unwrap(),
        "managed stdio EOF is not original Child exit"
    );
    if natural {
        let mut release = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        release.write_all(b"finish\n").unwrap();
    } else {
        owner.stop();
    }
    // The same parent RPC event progression sends EXIT only after actual
    // NOTE_EXIT. It never blocks waiting for the helper's unfinished Finish.
    loop {
        supervisor.advance().unwrap();
        if receiving
            .progress(source.as_raw_fd(), &mut [0; 4096], &memory, 131072)
            .unwrap()
        {
            break;
        }
        let mut descriptors = [
            libc::pollfd {
                fd: source.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: supervisor.poll_fd().unwrap(),
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
        assert!(!left.is_zero());
        assert!(
            unsafe {
                libc::poll(
                    descriptors.as_mut_ptr(),
                    descriptors.len() as _,
                    left.as_millis().min(i32::MAX as u128) as i32,
                )
            } > 0
        );
    }
    let done = receiving.finished().unwrap();
    assert_eq!(done.wait_errno, 0);
    assert_eq!(done.cancel_errno, 0);
    assert_eq!(done.close_errnos, [0; 10]);
    assert!(done.child_pid > 0);
    while !supervisor
        .completed_report(902, done.child_pid, done.operation_errno)
        .unwrap()
    {
        poll(&supervisor, deadline);
    }
    let result = worker.join().unwrap();
    if natural {
        result.unwrap();
        assert!(receiving.take().unwrap().status.success());
    } else {
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::Interrupted);
        assert!(receiving.take().is_err());
    }
    supervisor.close().unwrap();
    assert!(supervisor.closed());
    drop(context);
}
