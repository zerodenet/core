//! Narrow macOS authorization bridge for TUN network mutations.
//!
//! The Zero process remains owned by the signed-in user. A short-lived
//! AppleScript authorization starts one private helper for the TUN lifecycle;
//! the helper creates the utun descriptor and executes only the native network
//! tools used by this crate.

use std::fs;
use std::io::{self, BufRead, BufReader, Write};
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

const PRIVILEGED_COMMAND_SCRIPT: &str = r#"
on run argv
    if (count of argv) is 0 then error "missing privileged command"
    set commandText to quoted form of (item 1 of argv)
    repeat with argumentIndex from 2 to (count of argv)
        set commandText to commandText & " " & quoted form of (item argumentIndex of argv)
    end repeat
    do shell script commandText with administrator privileges
end run
"#;

static UTUN_HELPER_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static PRIVILEGED_HELPER: OnceLock<Mutex<Option<UnixStream>>> = OnceLock::new();

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HelperRequest {
    Command {
        program: HelperProgram,
        arguments: Vec<String>,
        input: Option<String>,
    },
    Shutdown,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum HelperProgram {
    Ifconfig,
    Route,
    Pfctl,
}

#[derive(Deserialize, Serialize)]
struct HelperResponse {
    success: bool,
    code: i32,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

struct HelperSocketGuard {
    directory: PathBuf,
    socket: PathBuf,
}

impl Drop for HelperSocketGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.socket);
        let _ = fs::remove_dir(&self.directory);
    }
}

pub(crate) fn request_utun(name: Option<&str>) -> io::Result<RawFd> {
    shutdown_existing_helper();
    let (listener, guard) = bind_helper_socket()?;
    let executable = std::env::current_exe()
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("resolve Zero executable for macOS TUN authorization: {error}"),
            )
        })?
        .to_string_lossy()
        .into_owned();
    let mut arguments = vec![
        "__macos-tun-create-helper".to_owned(),
        "--socket".to_owned(),
        guard.socket.to_string_lossy().into_owned(),
    ];
    if let Some(name) = name {
        arguments.extend(["--name".to_owned(), name.to_owned()]);
    }

    let mut child = Command::new("/usr/bin/osascript")
        .args(["-e", PRIVILEGED_COMMAND_SCRIPT, "--", executable.as_str()])
        .args(&arguments)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("request macOS administrator authorization for utun: {error}"),
            )
        })?;

    listener.set_nonblocking(true)?;
    let stream = loop {
        match listener.accept() {
            Ok((stream, _)) if peer_effective_uid(&stream)? == 0 => {
                // The listener is non-blocking only so the parent can observe
                // an authorization cancellation while waiting for the helper.
                // On macOS the accepted socket can retain that mode. All
                // helper requests below use blocking request/response I/O, so
                // leaving it enabled races the helper response and surfaces a
                // misleading EAGAIN as if `/sbin/ifconfig` failed to spawn.
                break blocking_helper_stream(stream)?;
            }
            Ok((_stream, _)) => continue,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {}
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("accept authorized utun descriptor: {error}"),
                ))
            }
        }
        if child.try_wait()?.is_some() {
            let output = child.wait_with_output()?;
            return Err(command_failure(
                "create utun with administrator authorization",
                &output,
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    };

    std::thread::spawn(move || {
        let _ = child.wait();
    });
    let fd = receive_fd(stream.as_raw_fd())?;
    *helper_slot().lock().map_err(|_| helper_lock_error())? = Some(stream);
    Ok(fd)
}

fn blocking_helper_stream(stream: UnixStream) -> io::Result<UnixStream> {
    stream.set_nonblocking(false)?;
    Ok(stream)
}

fn peer_effective_uid(stream: &UnixStream) -> io::Result<libc::uid_t> {
    let mut uid = 0;
    let mut gid = 0;
    if unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(uid)
}

/// Entry point used only by Zero's private macOS helper CLI command.
pub fn run_utun_create_helper(socket_path: &Path, name: Option<&str>) -> io::Result<()> {
    if unsafe { libc::geteuid() } != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "macOS utun helper must run with administrator privileges",
        ));
    }
    let fd = crate::macos::create_raw_utun(name)?;
    match UnixStream::connect(socket_path) {
        Ok(mut stream) => {
            let transfer = send_fd(stream.as_raw_fd(), fd);
            // SCM_RIGHTS duplicated the descriptor into the unprivileged process.
            // Close the helper's copy immediately so dropping the device there
            // tears the utun interface down without waiting for helper shutdown.
            unsafe { libc::close(fd) };
            transfer.and_then(|()| serve_helper(&mut stream))
        }
        Err(error) => {
            unsafe { libc::close(fd) };
            Err(error)
        }
    }
}

fn serve_helper(stream: &mut UnixStream) -> io::Result<()> {
    let mut reader = BufReader::new(stream.try_clone()?);
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(());
        }
        let request: HelperRequest = serde_json::from_str(&line).map_err(io::Error::other)?;
        let HelperRequest::Command {
            program,
            arguments,
            input,
        } = request
        else {
            return Ok(());
        };
        let output = run_as_root(program, &arguments, input.as_deref())?;
        let response = HelperResponse {
            success: output.status.success(),
            code: output.status.code().unwrap_or(1),
            stdout: output.stdout,
            stderr: output.stderr,
        };
        serde_json::to_writer(&mut *stream, &response).map_err(io::Error::other)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
    }
}

fn run_as_root(
    program: HelperProgram,
    arguments: &[String],
    input: Option<&str>,
) -> io::Result<Output> {
    let program = match program {
        HelperProgram::Ifconfig => "/sbin/ifconfig",
        HelperProgram::Route => "/sbin/route",
        HelperProgram::Pfctl => "/sbin/pfctl",
    };
    if let Some(input) = input {
        let mut child = Command::new(program)
            .args(arguments)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        child
            .stdin
            .take()
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "command stdin unavailable"))?
            .write_all(input.as_bytes())?;
        child.wait_with_output()
    } else {
        Command::new(program).args(arguments).output()
    }
}

fn bind_helper_socket() -> io::Result<(UnixListener, HelperSocketGuard)> {
    let base = std::env::temp_dir();
    for _ in 0..32 {
        let sequence = UTUN_HELPER_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let directory = base.join(format!(
            "zero-utun-{}-{}-{sequence}",
            unsafe { libc::geteuid() },
            std::process::id()
        ));
        match fs::create_dir(&directory) {
            Ok(()) => {
                let socket = directory.join("descriptor.sock");
                let guard = HelperSocketGuard { directory, socket };
                fs::set_permissions(&guard.directory, fs::Permissions::from_mode(0o700))?;
                let listener = UnixListener::bind(&guard.socket)?;
                fs::set_permissions(&guard.socket, fs::Permissions::from_mode(0o600))?;
                return Ok((listener, guard));
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not allocate a private macOS utun helper socket",
    ))
}

fn send_fd(socket: RawFd, fd: RawFd) -> io::Result<()> {
    let mut marker = [1_u8];
    let mut iovec = libc::iovec {
        iov_base: marker.as_mut_ptr().cast(),
        iov_len: marker.len(),
    };
    let control_len = unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) } as usize;
    let mut control = vec![0_u8; control_len];
    let mut message: libc::msghdr = unsafe { mem::zeroed() };
    message.msg_iov = &mut iovec;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len() as _;
    unsafe {
        let header = libc::CMSG_FIRSTHDR(&message);
        if header.is_null() {
            return Err(io::Error::other("allocate utun descriptor control message"));
        }
        (*header).cmsg_level = libc::SOL_SOCKET;
        (*header).cmsg_type = libc::SCM_RIGHTS;
        (*header).cmsg_len = libc::CMSG_LEN(mem::size_of::<RawFd>() as u32) as _;
        std::ptr::write(libc::CMSG_DATA(header).cast::<RawFd>(), fd);
        if libc::sendmsg(socket, &message, 0) < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn receive_fd(socket: RawFd) -> io::Result<RawFd> {
    let mut marker = [0_u8];
    let mut iovec = libc::iovec {
        iov_base: marker.as_mut_ptr().cast(),
        iov_len: marker.len(),
    };
    let control_len = unsafe { libc::CMSG_SPACE(mem::size_of::<RawFd>() as u32) } as usize;
    let mut control = vec![0_u8; control_len];
    let mut message: libc::msghdr = unsafe { mem::zeroed() };
    message.msg_iov = &mut iovec;
    message.msg_iovlen = 1;
    message.msg_control = control.as_mut_ptr().cast();
    message.msg_controllen = control.len() as _;
    let received = unsafe { libc::recvmsg(socket, &mut message, 0) };
    if received < 0 {
        return Err(io::Error::last_os_error());
    }
    if received == 0 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "authorized utun helper closed before returning a descriptor",
        ));
    }
    if message.msg_flags & libc::MSG_CTRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authorized utun descriptor control message was truncated",
        ));
    }
    let header = unsafe { libc::CMSG_FIRSTHDR(&message) };
    if header.is_null()
        || unsafe { (*header).cmsg_level } != libc::SOL_SOCKET
        || unsafe { (*header).cmsg_type } != libc::SCM_RIGHTS
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authorized utun helper did not return a file descriptor",
        ));
    }
    let fd = unsafe { std::ptr::read(libc::CMSG_DATA(header).cast::<RawFd>()) };
    if fd < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "authorized utun helper returned an invalid file descriptor",
        ));
    }
    if unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) } < 0 {
        let error = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(error);
    }
    Ok(fd)
}

pub(crate) fn output(program: &str, arguments: &[String]) -> io::Result<Output> {
    if unsafe { libc::geteuid() } == 0 {
        return Command::new(program).args(arguments).output();
    }
    helper_output(program, arguments, None)
}

pub(crate) fn output_with_input(
    program: &str,
    arguments: &[String],
    input: &str,
) -> io::Result<Output> {
    if unsafe { libc::geteuid() } == 0 {
        return run_as_root(helper_program(program)?, arguments, Some(input));
    }
    helper_output(program, arguments, Some(input))
}

fn helper_output(program: &str, arguments: &[String], input: Option<&str>) -> io::Result<Output> {
    let request = HelperRequest::Command {
        program: helper_program(program)?,
        arguments: arguments.to_vec(),
        input: input.map(str::to_owned),
    };
    let mut slot = helper_slot().lock().map_err(|_| helper_lock_error())?;
    let stream = slot.as_mut().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotConnected,
            "macOS privileged TUN helper is not connected",
        )
    })?;
    let result = (|| {
        serde_json::to_writer(&mut *stream, &request).map_err(io::Error::other)?;
        stream.write_all(b"\n")?;
        stream.flush()?;
        let mut line = String::new();
        BufReader::new(&mut *stream).read_line(&mut line)?;
        if line.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "macOS privileged TUN helper closed the control channel",
            ));
        }
        let response: HelperResponse = serde_json::from_str(&line).map_err(io::Error::other)?;
        Ok(Output {
            status: exit_status(response.success, response.code),
            stdout: response.stdout,
            stderr: response.stderr,
        })
    })();
    if result.is_err() {
        slot.take();
    }
    result
}

fn helper_program(program: &str) -> io::Result<HelperProgram> {
    match Path::new(program)
        .file_name()
        .and_then(|name| name.to_str())
    {
        Some("ifconfig") => Ok(HelperProgram::Ifconfig),
        Some("route") => Ok(HelperProgram::Route),
        Some("pfctl") => Ok(HelperProgram::Pfctl),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unsupported macOS privileged TUN program `{program}`"),
        )),
    }
}

fn helper_slot() -> &'static Mutex<Option<UnixStream>> {
    PRIVILEGED_HELPER.get_or_init(|| Mutex::new(None))
}

fn shutdown_existing_helper() {
    let Ok(mut slot) = helper_slot().lock() else {
        return;
    };
    if let Some(mut stream) = slot.take() {
        let _ = serde_json::to_writer(&mut stream, &HelperRequest::Shutdown);
        let _ = stream.write_all(b"\n");
        let _ = stream.flush();
    }
}

fn helper_lock_error() -> io::Error {
    io::Error::other("macOS privileged TUN helper lock is poisoned")
}

fn exit_status(success: bool, code: i32) -> ExitStatus {
    use std::os::unix::process::ExitStatusExt;

    ExitStatus::from_raw(if success { 0 } else { code.max(1) << 8 })
}

fn command_failure(action: &str, output: &Output) -> io::Error {
    let detail = if output.stderr.is_empty() {
        String::from_utf8_lossy(&output.stdout)
    } else {
        String::from_utf8_lossy(&output.stderr)
    };
    io::Error::other(format!("{action} failed: {}", detail.trim()))
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::os::fd::AsRawFd;
    use std::os::unix::net::UnixStream;

    use super::{blocking_helper_stream, receive_fd, send_fd, PRIVILEGED_COMMAND_SCRIPT};

    #[test]
    fn privileged_script_shell_quotes_every_external_value() {
        assert!(PRIVILEGED_COMMAND_SCRIPT.contains("quoted form of"));
        assert!(PRIVILEGED_COMMAND_SCRIPT.contains("with administrator privileges"));
    }

    #[test]
    fn transfers_a_file_descriptor_over_the_private_helper_socket() {
        let (sender, receiver) = UnixStream::pair().expect("create descriptor transport");
        let file = File::open("/dev/null").expect("open descriptor fixture");
        send_fd(sender.as_raw_fd(), file.as_raw_fd()).expect("send descriptor");
        let received = receive_fd(receiver.as_raw_fd()).expect("receive descriptor");
        assert!(received >= 0);
        unsafe { libc::close(received) };
    }

    #[test]
    fn helper_control_stream_is_restored_to_blocking_io() {
        let (stream, _peer) = UnixStream::pair().expect("create helper control stream");
        stream
            .set_nonblocking(true)
            .expect("make helper control stream non-blocking");

        let stream = blocking_helper_stream(stream).expect("restore blocking helper stream");
        let flags = unsafe { libc::fcntl(stream.as_raw_fd(), libc::F_GETFL) };

        assert!(flags >= 0);
        assert_eq!(flags & libc::O_NONBLOCK, 0);
    }
}
