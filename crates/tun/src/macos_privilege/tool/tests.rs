use super::*;
use std::{os::fd::FromRawFd, os::fd::OwnedFd, time::Duration, time::Instant};

#[test]
fn actual_fixed_tool_reports_its_own_pid_before_exec_and_waits_for_original_event_registration() {
    let mut supervisor = Supervisor::new();
    supervisor.prepare(81).unwrap();
    let raw = unsafe { libc::dup(supervisor.remote_fd().unwrap()) };
    assert!(raw >= 0);
    let remote = unsafe { OwnedFd::from_raw_fd(raw) };
    supervisor.begin(82).unwrap();
    let worker = std::thread::spawn(move || {
        let mut child = ChildOriginal::new();
        let result = run(
            &mut child,
            HelperProgram::Route,
            &["-n".to_owned(), "get".to_owned(), "127.0.0.1".to_owned()],
            None,
            Some((remote.as_raw_fd(), 82)),
            65_536,
        );
        assert!(child.reaped());
        assert!(child.reusable());
        result
    });
    let deadline = Instant::now() + Duration::from_secs(5);
    while !supervisor.completed().unwrap() {
        assert!(
            Instant::now() < deadline,
            "same real command has no matching exit receipt"
        );
        let mut descriptors = [
            libc::pollfd {
                fd: supervisor.poll_fd().unwrap(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: supervisor.event_fd().unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        assert!(unsafe { libc::poll(descriptors.as_mut_ptr(), descriptors.len() as _, 100) } >= 0);
    }
    let _output = worker.join().unwrap().unwrap();
    supervisor.close().unwrap();
    assert!(supervisor.closed());
}

#[test]
fn fixed_local_query_reaps_its_actual_child_and_explicitly_closes_all_preinstalled_pipe_ends() {
    let mut child = ChildOriginal::new();
    let output = run_query(
        &mut child,
        Query::Route(vec!["-n".into(), "get".into(), "127.0.0.1".into()]),
        65_536,
    )
    .unwrap();
    assert!(child.reaped());
    assert!(child.reusable());
    assert!(!output.stdout.is_empty() || !output.stderr.is_empty());
    let second = run_query(&mut child, Query::SystemDns, 65_536).unwrap();
    assert!(child.reaped());
    assert!(child.reusable());
    drop(second);
}
