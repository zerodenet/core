//! Authorization is short-lived; the private control socket owns helper lifetime.
use std::{
    io,
    os::unix::net::{UnixListener, UnixStream},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use super::{bind_helper_socket, blocking_helper_stream, command_failure, peer_effective_uid};

pub(super) const PRIVILEGED_COMMAND_SCRIPT: &str = r#"
on run argv
    if (count of argv) is 0 then error "missing privileged command"
    set commandText to quoted form of (item 1 of argv)
    repeat with argumentIndex from 2 to (count of argv)
        set commandText to commandText & " " & quoted form of (item argumentIndex of argv)
    end repeat
    do shell script commandText & " </dev/null >/dev/null 2>&1 &" with administrator privileges
end run
"#;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

pub(super) fn launch_authorized_helper() -> io::Result<UnixStream> {
    let (listener, guard) = bind_helper_socket()?;
    let executable = std::env::current_exe()?;
    let mut child = Command::new("/usr/bin/osascript")
        .args(["-e", PRIVILEGED_COMMAND_SCRIPT, "--"])
        .arg(executable)
        .args(["__macos-tun-create-helper", "--socket"])
        .arg(&guard.socket)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let result = await_helper(&listener, &mut child, 0, CONNECT_TIMEOUT);
    // Reap even when the helper connects before the authorization command exits.
    // This process only launches the detached helper, never waits for its lifetime.
    std::thread::spawn(move || {
        let _ = child.wait();
    });
    result
}

fn await_helper(
    listener: &UnixListener,
    child: &mut Child,
    expected_uid: libc::uid_t,
    timeout: Duration,
) -> io::Result<UnixStream> {
    listener.set_nonblocking(true)?;
    let mut deadline = None;
    loop {
        // An authorization denial cannot be accepted as a successful launch,
        // even if an unrelated process has connected to the private socket.
        if let Some(status) = child.try_wait()? {
            if !status.success() {
                let mut stderr = String::new();
                if let Some(mut pipe) = child.stderr.take() {
                    use std::io::Read;
                    pipe.read_to_string(&mut stderr)?;
                }
                return Err(command_failure(
                    "create utun with administrator authorization",
                    &std::process::Output {
                        status,
                        stdout: Vec::new(),
                        stderr: stderr.into_bytes(),
                    },
                ));
            }
            // The user may take time to authorize. Bound only the subsequent
            // helper connection, including missing/non-executable helper images.
            deadline.get_or_insert_with(|| Instant::now() + timeout);
        }
        match listener.accept() {
            Ok((stream, _)) if peer_effective_uid(&stream)? == expected_uid => {
                return blocking_helper_stream(stream);
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(error),
        }
        if deadline.is_some_and(|at| Instant::now() >= at) {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "authorized macOS TUN helper did not connect",
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(test)]
mod tests;
