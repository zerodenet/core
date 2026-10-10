use std::{io::Write, os::unix::net::UnixListener, process::Command, time::Instant};

use super::{await_helper, Duration, PRIVILEGED_COMMAND_SCRIPT};

fn exited_child(code: i32) -> std::process::Child {
    Command::new("/bin/sh")
        .args(["-c", &format!("exit {code}")])
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap()
}

#[test]
fn successful_authorizer_exit_is_not_helper_connection_failure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let mut child = exited_child(0);
    assert!(child.wait().unwrap().success());
    let client = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(75));
        std::os::unix::net::UnixStream::connect(path).unwrap()
    });
    let stream = await_helper(
        &listener,
        &mut child,
        unsafe { libc::geteuid() },
        Duration::from_secs(2),
    )
    .unwrap();
    let flags = unsafe { libc::fcntl(std::os::fd::AsRawFd::as_raw_fd(&stream), libc::F_GETFL) };
    assert_eq!(flags & libc::O_NONBLOCK, 0);
    drop(client.join().unwrap());
}

#[test]
fn missing_helper_after_successful_authorization_times_out() {
    let dir = tempfile::tempdir().unwrap();
    let listener = UnixListener::bind(dir.path().join("control.sock")).unwrap();
    let mut child = exited_child(0);
    child.wait().unwrap();
    let error = await_helper(
        &listener,
        &mut child,
        unsafe { libc::geteuid() },
        Duration::ZERO,
    )
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
}

#[test]
fn authorization_failure_and_wrong_peer_are_not_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("control.sock");
    let listener = UnixListener::bind(&path).unwrap();
    let _wrong_peer = std::os::unix::net::UnixStream::connect(&path).unwrap();
    let mut denied = exited_child(1);
    denied.wait().unwrap();
    let error = await_helper(
        &listener,
        &mut denied,
        unsafe { libc::geteuid() },
        Duration::ZERO,
    )
    .unwrap_err();
    assert!(error.to_string().contains("authorization"));
    let mut successful = exited_child(0);
    successful.wait().unwrap();
    let wrong_uid = unsafe { libc::geteuid() }.wrapping_add(1);
    assert_eq!(
        await_helper(&listener, &mut successful, wrong_uid, Duration::ZERO)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::TimedOut
    );
}

#[test]
fn applescript_exits_while_detached_child_waits_for_control_input() {
    let dir = tempfile::tempdir().unwrap();
    let ready = dir.path().join("ready");
    let control = dir.path().join("control");
    let finished = dir.path().join("finished");
    let fifo = std::ffi::CString::new(control.as_os_str().as_encoded_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo.as_ptr(), 0o600) }, 0);
    // Exercise exact quoting/background launch without admin prompts or network
    // mutations. A FIFO keeps the fixture alive until this test releases it.
    let script = PRIVILEGED_COMMAND_SCRIPT.replace(" with administrator privileges", "");
    let fixture = "printf ready > \"$1\"; IFS= read -r command < \"$2\"; printf closed > \"$3\"";
    let mut child = Command::new("/usr/bin/osascript")
        .args(["-e", &script, "--", "/bin/sh", "-c", fixture, "--"])
        .arg(&ready)
        .arg(&control)
        .arg(&finished)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !ready.exists() {
        assert!(Instant::now() < deadline, "detached fixture did not start");
        std::thread::sleep(Duration::from_millis(25));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut exited = None;
    while Instant::now() < deadline {
        exited = child.try_wait().unwrap();
        if exited.is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    let was_waiting = !finished.exists();
    // Release the fixture before asserting, including on a failed timing check.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&control)
        .unwrap()
        .write_all(b"close\n")
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !finished.exists() {
        assert!(
            Instant::now() < deadline,
            "fixture did not stop on control input"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    child.wait().unwrap();
    assert!(
        was_waiting,
        "fixture must still be alive when authorizer exits"
    );
    assert!(
        exited.is_some_and(|status| status.success()),
        "authorizer retained child lifetime"
    );
}
