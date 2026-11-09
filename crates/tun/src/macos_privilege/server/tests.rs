use super::*;
use std::time::{Duration, Instant};

#[test]
fn actual_owned_helper_protocol_retains_supervisor_and_fixed_tool_before_reply_and_close() {
    let (mut parent, mut helper) = UnixStream::pair().unwrap();
    let worker = std::thread::spawn(move || serve(&mut helper));
    let root = std::sync::Arc::new(crate::packet_memory::PacketReturnTotals::default());
    let grant = root.native_grant(64).unwrap();
    let memory = grant.memory_view().unwrap();
    let owner = session::MacosHelperOwner::prepare(&memory, 65_536, false, 65_536).unwrap();
    let _context = owner.enter().unwrap();
    let mode = owner.test_command_mode().unwrap();
    let storage = owner.test_output_storage().unwrap();
    let mut supervisor = tool::Supervisor::from_source(mode.control().clone());
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
    supervisor.begin_source(102, &mode).unwrap();
    let mut receiving = command::receive::Receiving::new(102, &storage).unwrap();
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
    parent.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        supervisor.advance().unwrap();
        if receiving
            .progress(parent.as_raw_fd(), &mut [0; 4096], &memory, 65_536)
            .unwrap()
        {
            let done = receiving.finished().unwrap();
            if supervisor
                .completed_report(102, done.child_pid, done.operation_errno)
                .unwrap()
            {
                break;
            }
        }
        let left = deadline.saturating_duration_since(Instant::now());
        assert!(!left.is_zero(), "actual helper tool not retired");
        let mut fds = [
            libc::pollfd {
                fd: parent.as_raw_fd(),
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
        assert!(
            unsafe {
                libc::poll(
                    fds.as_mut_ptr(),
                    fds.len() as _,
                    (left.as_millis().min(i32::MAX as u128) as i32).max(1),
                )
            } > 0
        );
    }
    let actual_output = receiving.take().unwrap();
    assert!(!actual_output.stdout.is_empty() || !actual_output.stderr.is_empty());
    assert!(supervisor.completed().unwrap());
    parent.set_nonblocking(false).unwrap();
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
