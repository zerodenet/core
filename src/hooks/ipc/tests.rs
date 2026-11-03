use super::*;
use std::{
    io::Read,
    os::unix::net::UnixListener,
    sync::{mpsc, Arc, Mutex},
};
fn context() -> FlowContext {
    FlowContext {
        flow_id: 41,
        inbound_tag: Some("direct".into()),
        outbound_tag: None,
        target_host: "example.test".into(),
        target_port: 443,
        network: "tcp".into(),
        protocol: "direct".into(),
        auth: None,
        mode: "rule".into(),
        started_at_unix_ms: 7,
        labels: Vec::new(),
    }
}
#[test]
fn actual_start_deny_allow_and_end_wire_use_new_originals_and_matching_native_closes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("hook.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = std::thread::spawn(move || {
        let mut requests = Vec::new();
        for index in 0..3 {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut line = String::new();
            BufReader::new(&mut stream).read_line(&mut line).unwrap();
            assert!(line.ends_with('\n'));
            requests.push(line.into_bytes());
            if index == 0 {
                stream.write_all(b"{\"allow\":false,\"code\":\"real-deny\",\"message\":\"original decision\"}\n").unwrap();
            }
            if index == 1 {
                stream.write_all(b"{\"allow\":true}\n").unwrap();
            }
            let mut byte = [0];
            assert_eq!(
                stream.read(&mut byte).unwrap(),
                0,
                "no idle cloned descriptor may keep this original alive"
            );
        }
        requests
    });
    let hook = IpcFlowHook::new(path.display().to_string(), Duration::from_secs(3));
    let denied = hook.on_flow_start(&context()).unwrap_err();
    assert_eq!(denied.code, "real-deny");
    assert_eq!(denied.message, "original decision");
    assert_eq!(hook.original.observation(), (1, 0, false, false));
    hook.on_flow_start(&context()).unwrap();
    assert_eq!(hook.original.observation(), (2, 0, false, false));
    hook.on_flow_end(
        &context(),
        SessionOutcome::DirectRelayed,
        &FlowTraffic {
            bytes_up: 11,
            bytes_down: 17,
            duration_ms: 29,
        },
    );
    assert_eq!(hook.original.observation(), (3, 0, false, false));
    hook.retire().unwrap();
    let requests = server.join().unwrap();
    let start = serde_json::json!({"type":"check_flow","flow_id":41,"inbound_tag":"direct",
        "target_host":"example.test","target_port":443,"network":"tcp","protocol":"direct",
        "auth_scheme":null,"principal_key":null,"mode":"rule"});
    let mut start_bytes = serde_json::to_vec(&start).unwrap();
    start_bytes.push(b'\n');
    assert_eq!(requests[0], start_bytes);
    assert_eq!(requests[1], start_bytes);
    let expected = serde_json::json!({"type":"flow_end","flow_id":41,"inbound_tag":"direct",
        "target_host":"example.test","target_port":443,"network":"tcp","principal_key":null,
        "outcome":"direct_relayed","bytes_up":11,"bytes_down":17,"duration_ms":29});
    let mut bytes = serde_json::to_vec(&expected).unwrap();
    bytes.push(b'\n');
    assert_eq!(
        requests[2], bytes,
        "preserve the original end JSON bytes and newline"
    );
}

#[test]
fn failed_real_connect_is_fail_open_but_its_acquired_socket_still_closes() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("missing.sock");
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let captured = warnings.clone();
    let hook = IpcFlowHook::new(path.display().to_string(), Duration::from_millis(100))
        .with_warning_handler(move |code, _| captured.lock().unwrap().push(code.to_owned()));
    hook.on_flow_start(&context()).unwrap();
    assert_eq!(hook.original.observation(), (1, 0, false, false));
    assert_eq!(
        warnings.lock().unwrap().as_slice(),
        ["ipc_hook_unreachable"]
    );
    hook.retire().unwrap();
    hook.on_flow_start(&context()).unwrap(); // retired callback cannot construct another original
    assert_eq!(hook.original.observation(), (1, 0, false, false));
}

#[test]
fn retirement_waits_for_the_real_callback_return_separately_from_native_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("callback.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut line = String::new();
        BufReader::new(&mut stream).read_line(&mut line).unwrap();
        stream.write_all(b"not JSON\n").unwrap();
    });
    let (entered, callback_entered) = mpsc::channel();
    let (release, released) = mpsc::channel();
    let released = Mutex::new(released);
    let hook = Arc::new(
        IpcFlowHook::new(path.display().to_string(), Duration::from_secs(3)).with_warning_handler(
            move |_, _| {
                entered.send(()).unwrap();
                released.lock().unwrap().recv().unwrap();
            },
        ),
    );
    let callback_hook = hook.clone();
    let callback = std::thread::spawn(move || callback_hook.on_flow_start(&context()));
    callback_entered
        .recv_timeout(Duration::from_secs(3))
        .unwrap();
    assert_eq!(hook.original.observation(), (1, 0, false, false)); // native already closed
    let (done, completion) = mpsc::channel();
    let retired_hook = hook.clone();
    let retirement = std::thread::spawn(move || {
        let result = retired_hook.retire();
        done.send(()).unwrap();
        result
    });
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    while !hook.is_retired() {
        assert!(std::time::Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert!(
        matches!(completion.try_recv(), Err(mpsc::TryRecvError::Empty)),
        "native receipt is not a callback return"
    );
    release.send(()).unwrap();
    callback.join().unwrap().unwrap();
    retirement.join().unwrap().unwrap();
    server.join().unwrap();
    assert_eq!(hook.original.observation(), (1, 0, false, false));
}

#[test]
fn a_confirmed_actual_deny_survives_an_independent_cleanup_diagnostic_failure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("deny.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut line = String::new();
        BufReader::new(&mut stream).read_line(&mut line).unwrap();
        stream
            .write_all(
                b"{\"allow\":false,\"code\":\"confirmed-deny\",\"message\":\"keep decision\"}\n",
            )
            .unwrap();
    });
    let hook = IpcFlowHook::new(path.display().to_string(), Duration::from_secs(3))
        .with_warning_handler(|_, _| panic!("diagnostic handler failed"));
    let mut actual = hook.send_recv(&serde_json::json!({"type":"check_flow"}));
    assert!(actual.cleanup.is_ok());
    assert_eq!(hook.original.observation(), (1, 0, false, false));
    server.join().unwrap();
    // Decision/error-channel regression only: inject a diagnostic failure
    // after the real response/native close. This does not qualify an unknown
    // native close and constructs no close receipt or simulated successful IO.
    actual.cleanup =
        Err(std::fs::File::open(directory.path().join("missing-cleanup-evidence")).unwrap_err());
    let decision = hook.resolve_start_call(actual).unwrap_err();
    assert_eq!(decision.code, "confirmed-deny");
    assert_eq!(decision.message, "keep decision");
    hook.retire().unwrap();
}
