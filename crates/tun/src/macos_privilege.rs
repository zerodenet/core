//! Narrow macOS authorization bridge for TUN network mutations.
//!
//! The Zero process remains owned by the signed-in user. A short-lived
//! AppleScript authorization starts one private helper for the TUN lifecycle;
//! the helper creates the utun descriptor and executes only the native network
//! tools used by this crate.

use std::fs;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::mem;
use std::os::fd::{AsRawFd, RawFd};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

mod launch;
use launch::launch_authorized_helper;
#[cfg(test)]
use launch::PRIVILEGED_COMMAND_SCRIPT;

static UTUN_HELPER_SEQUENCE: AtomicU64 = AtomicU64::new(0);
static PRIVILEGED_HELPER: OnceLock<Mutex<Option<UnixStream>>> = OnceLock::new();

#[derive(Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum HelperRequest {
    CreateUtun {
        name: Option<String>,
    },
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
    let mut slot = helper_slot().lock().map_err(|_| helper_lock_error())?;
    if let Some(stream) = slot.as_mut() {
        match request_helper_utun(stream, name) {
            Ok(fd) => return Ok(fd),
            Err(error) if helper_connection_lost(&error) => {
                slot.take();
            }
            Err(error) => return Err(error),
        }
    }

    let mut stream = launch_authorized_helper()?;
    let fd = request_helper_utun(&mut stream, name)?;
    *slot = Some(stream);
    Ok(fd)
}

fn request_helper_utun(stream: &mut UnixStream, name: Option<&str>) -> io::Result<RawFd> {
    let request = HelperRequest::CreateUtun {
        name: name.map(str::to_owned),
    };
    serde_json::to_writer(&mut *stream, &request).map_err(io::Error::other)?;
    stream.write_all(b"\n")?;
    stream.flush()?;

    let response = read_helper_response(stream)?;
    if !response.success {
        return Err(helper_response_error("create macOS utun", &response));
    }
    receive_fd(stream.as_raw_fd())
}

fn helper_connection_lost(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::NotConnected
            | io::ErrorKind::UnexpectedEof
    )
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
    match UnixStream::connect(socket_path) {
        Ok(mut stream) => {
            // The optional name is retained for compatibility with older
            // private invocations. New parents send every create request over
            // the persistent channel so one authorization helper can serve
            // repeated TUN starts for the lifetime of the Zero process.
            if name.is_some() {
                create_utun_for_parent(&mut stream, name)?;
            }
            serve_helper(&mut stream)
        }
        Err(error) => Err(error),
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
        match request {
            HelperRequest::CreateUtun { name } => {
                create_utun_for_parent(stream, name.as_deref())?;
            }
            HelperRequest::Command {
                program,
                arguments,
                input,
            } => {
                let output = run_as_root(program, &arguments, input.as_deref())?;
                write_helper_response(
                    stream,
                    &HelperResponse {
                        success: output.status.success(),
                        code: output.status.code().unwrap_or(1),
                        stdout: output.stdout,
                        stderr: output.stderr,
                    },
                )?;
            }
            HelperRequest::Shutdown => return Ok(()),
        }
    }
}

fn create_utun_for_parent(stream: &mut UnixStream, name: Option<&str>) -> io::Result<()> {
    match crate::macos::create_raw_utun(name) {
        Ok(fd) => {
            write_helper_response(
                stream,
                &HelperResponse {
                    success: true,
                    code: 0,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                },
            )?;
            let transfer = send_fd(stream.as_raw_fd(), fd);
            // SCM_RIGHTS duplicated the descriptor into the unprivileged
            // process. The helper never retains a TUN descriptor.
            unsafe { libc::close(fd) };
            transfer
        }
        Err(error) => write_helper_response(
            stream,
            &HelperResponse {
                success: false,
                code: error.raw_os_error().unwrap_or(1),
                stdout: Vec::new(),
                stderr: error.to_string().into_bytes(),
            },
        ),
    }
}

fn write_helper_response(stream: &mut UnixStream, response: &HelperResponse) -> io::Result<()> {
    serde_json::to_writer(&mut *stream, response).map_err(io::Error::other)?;
    stream.write_all(b"\n")?;
    stream.flush()
}

// Read exactly through the response delimiter. A buffered reader is not used
// here because the next byte can carry SCM_RIGHTS ancillary data; prefetching
// that byte with plain read(2) would discard the transferred descriptor.
fn read_helper_response(stream: &mut UnixStream) -> io::Result<HelperResponse> {
    let mut line = Vec::new();
    loop {
        let mut byte = [0_u8; 1];
        if stream.read(&mut byte)? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "macOS privileged TUN helper closed the control channel",
            ));
        }
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
        if line.len() > 64 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "macOS privileged TUN helper response is too large",
            ));
        }
    }
    serde_json::from_slice(&line).map_err(io::Error::other)
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
        let response = read_helper_response(stream)?;
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

fn helper_response_error(action: &str, response: &HelperResponse) -> io::Error {
    let detail = if response.stderr.is_empty() {
        String::from_utf8_lossy(&response.stdout)
    } else {
        String::from_utf8_lossy(&response.stderr)
    };
    io::Error::other(format!("{action} failed: {}", detail.trim()))
}

#[cfg(test)]
#[path = "macos_privilege/tests/mod.rs"]
mod tests;
