use super::*;
use std::time::{Duration, Instant};

#[test]
fn actual_owned_helper_protocol_retains_supervisor_and_fixed_tool_before_reply_and_close() {
    let (mut parent, mut helper) = UnixStream::pair().unwrap();
    let worker = std::thread::spawn(move || serve(&mut helper));
    let mut supervisor = tool::Supervisor::new();
    supervisor.prepare(101).unwrap();
    serde_json::to_writer(
        &mut parent,
        &HelperRequest::InstallSupervisor {
            transfer: 101,
            request_capacity: 65_536,
        },
    )
    .unwrap();
    parent.write_all(b"\n").unwrap();
    send_fd(parent.as_raw_fd(), supervisor.remote_fd().unwrap()).unwrap();
    assert!(read_helper_response(&mut parent).unwrap().success);
    supervisor.finish_transfer(&mut parent).unwrap();
    supervisor.begin(102).unwrap();
    serde_json::to_writer(
        &mut parent,
        &HelperRequest::OwnedCommand {
            program: HelperProgram::Route,
            arguments: vec!["-n".into(), "get".into(), "127.0.0.1".into()],
            input: None,
            nonce: 102,
            output_limit: 65_536,
        },
    )
    .unwrap();
    parent.write_all(b"\n").unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !supervisor.completed().unwrap() {
        assert!(Instant::now() < deadline, "actual helper tool not retired");
        let mut fds = [
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
        assert!(unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, 100) } >= 0);
    }
    let _actual_output = read_helper_response(&mut parent).unwrap();
    serde_json::to_writer(&mut parent, &HelperRequest::Shutdown).unwrap();
    parent.write_all(b"\n").unwrap();
    worker.join().unwrap().unwrap();
    supervisor.close().unwrap();
    assert!(supervisor.closed());
}

#[test]
fn rejected_pure_utun_name_reports_exact_no_native_disposition_for_its_request() {
    let (mut parent, mut helper) = UnixStream::pair().unwrap();
    let worker = std::thread::spawn(move || serve(&mut helper));
    serde_json::to_writer(
        &mut parent,
        &HelperRequest::OwnedCreateUtun {
            name: Some("not-a-utun".into()),
            transfer: 103,
        },
    )
    .unwrap();
    parent.write_all(b"\n").unwrap();
    let response = read_helper_response(&mut parent).unwrap();
    assert!(!response.success);
    assert_eq!(&response.stdout[..8], &103_u64.to_be_bytes());
    assert_eq!(response.stdout[8], 1); // Actual pre-socket validation failure.
    assert_eq!(response.stdout.len(), 13);
    serde_json::to_writer(&mut parent, &HelperRequest::Shutdown).unwrap();
    parent.write_all(b"\n").unwrap();
    worker.join().unwrap().unwrap();
}
