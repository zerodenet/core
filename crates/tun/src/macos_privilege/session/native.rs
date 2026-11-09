//! Actual authorization Child, accepted control, listener and helper kqueue.
use super::*;
use std::{
    fs,
    mem::ManuallyDrop,
    os::fd::IntoRawFd,
    os::unix::{ffi::OsStrExt, fs::PermissionsExt},
    process::{Child, Command, ExitStatus, Stdio},
};
pub(super) struct Native {
    pub(super) listener: Option<Original>,
    pub(super) control: Option<Original>,
    pub(super) socket: Option<HelperSocketGuard>,
    pub(super) child: Option<Child>,
    pub(super) waited: Option<ExitStatus>,
    pub(super) auth_output: Option<Original>,
    pub(super) auth_bytes: [u8; 64],
    pub(super) auth_read: usize,
    pub(super) auth_eof: bool,
    pub(super) cancelled: bool,
    pub(super) helper: process::ProcessExit,
    pub(super) authenticated: bool,
    pub(super) attempted: bool,
    pub(super) exited: bool,
}
impl Native {
    pub(super) fn new() -> Self {
        Self {
            listener: None,
            control: None,
            socket: None,
            child: None,
            waited: None,
            auth_output: None,
            auth_bytes: [0; 64],
            auth_read: 0,
            auth_eof: false,
            cancelled: false,
            helper: process::ProcessExit::new(),
            authenticated: false,
            attempted: false,
            exited: false,
        }
    }
    pub(super) fn begin(&mut self, id: u64) -> io::Result<()> {
        if self.attempted {
            return Err(io::Error::other(
                "same authorization original already attempted",
            ));
        }
        self.attempted = true;
        let directory =
            std::env::temp_dir().join(format!("zero-owned-utun-{}-{id}", std::process::id()));
        fs::create_dir(&directory)?;
        let socket = directory.join("descriptor.sock");
        self.socket = Some(HelperSocketGuard { directory, socket });
        let path = self.socket.as_ref().unwrap();
        fs::set_permissions(&path.directory, fs::Permissions::from_mode(0o700))?;
        let bytes = path.socket.as_os_str().as_bytes();
        let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
        if bytes.len() >= address.sun_path.len() {
            return Err(io::Error::other("private helper socket path is too long"));
        }
        address.sun_family = libc::AF_UNIX as _;
        address.sun_len =
            (std::mem::offset_of!(libc::sockaddr_un, sun_path) + bytes.len() + 1) as _;
        for (slot, byte) in address.sun_path.iter_mut().zip(bytes) {
            *slot = *byte as _;
        }
        let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        self.listener = Some(Original::new(raw, id));
        if unsafe {
            libc::bind(
                raw,
                (&address as *const libc::sockaddr_un).cast(),
                address.sun_len as _,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        fs::set_permissions(&path.socket, fs::Permissions::from_mode(0o600))?;
        set_flags(raw)?;
        if unsafe { libc::listen(raw, 1) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let executable = std::env::current_exe()?;
        // Foreground command: the actual authorization Child is retained here,
        // no detached shell helper and no untracked child.wait thread.
        self.child = Some(
            Command::new("/usr/bin/osascript")
                .args(["-e", OWNED_SCRIPT, "--"])
                .arg(executable)
                .args(["__macos-tun-create-helper", "--socket"])
                .arg(&path.socket)
                .arg(id.to_string())
                .stdin(Stdio::inherit())
                .stdout(Stdio::piped())
                .stderr(Stdio::inherit())
                .spawn()?,
        );
        self.auth_output = self
            .child
            .as_mut()
            .unwrap()
            .stdout
            .take()
            .map(|pipe| Original::new(pipe.into_raw_fd(), id));
        if let Some(output) = &self.auth_output {
            let flags = unsafe { libc::fcntl(output.fd(), libc::F_GETFL) };
            if flags < 0
                || unsafe { libc::fcntl(output.fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
    pub(super) fn accept(&mut self, id: u64) -> io::Result<()> {
        if self.control.is_some() {
            return Ok(());
        }
        let listener = self
            .listener
            .as_ref()
            .filter(|original| original.usable())
            .map(Original::fd)
            .ok_or_else(|| io::Error::other("retained helper listener is unavailable"))?;
        loop {
            let raw = unsafe { libc::accept(listener, std::ptr::null_mut(), std::ptr::null_mut()) };
            if raw >= 0 {
                // Store the actual accepted FD before getpeereid/fcntl/pid IO.
                self.control = Some(Original::new(raw, id));
                set_flags(raw)?;
                let stream = unsafe { ManuallyDrop::new(UnixStream::from_raw_fd(raw)) };
                if peer_effective_uid(&stream)? != 0 {
                    return Err(io::Error::other(
                        "helper peer is not the authorized root process",
                    ));
                }
                let mut pid: libc::pid_t = 0;
                let mut length = std::mem::size_of_val(&pid) as libc::socklen_t;
                if unsafe {
                    libc::getsockopt(
                        raw,
                        libc::SOL_LOCAL,
                        libc::LOCAL_PEERPID,
                        (&mut pid as *mut libc::pid_t).cast(),
                        &mut length,
                    )
                } < 0
                {
                    return Err(io::Error::last_os_error());
                }
                if length as usize != std::mem::size_of_val(&pid) {
                    return Err(io::Error::other("helper PID observation is incomplete"));
                }
                self.helper.register(pid, id)?;
                return Ok(());
            }
            let source = io::Error::last_os_error();
            if source.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if source.kind() != io::ErrorKind::WouldBlock {
                return Err(source);
            }
            self.observe_authorization(id)?;
            // Waits inside the one actual blocking responsibility; cancellation
            // retains this Child/listener instead of pretending no helper exists.
            let mut fd = libc::pollfd {
                fd: listener,
                events: libc::POLLIN,
                revents: 0,
            };
            if unsafe { libc::poll(&mut fd, 1, 100) } < 0 {
                let source = io::Error::last_os_error();
                if source.kind() != io::ErrorKind::Interrupted {
                    return Err(source);
                }
            }
        }
    }
    pub(super) fn control_fd(&self) -> io::Result<RawFd> {
        self.control
            .as_ref()
            .filter(|original| original.usable())
            .map(Original::fd)
            .ok_or_else(|| io::Error::other("original helper control is unavailable"))
    }
    pub(super) fn authenticate(&mut self, id: u64, response: &HelperResponse) -> io::Result<()> {
        if !response.success || response.stdout.as_slice() != id.to_be_bytes() {
            return Err(io::Error::other(
                "helper post-registration nonce does not match",
            ));
        }
        // The same authenticated control peer just answered after registration.
        let raw = self.control_fd()?;
        let mut pid: libc::pid_t = 0;
        let mut length = std::mem::size_of_val(&pid) as libc::socklen_t;
        if unsafe {
            libc::getsockopt(
                raw,
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                (&mut pid as *mut libc::pid_t).cast(),
                &mut length,
            )
        } < 0
        {
            return Err(io::Error::last_os_error());
        }
        self.helper.observe_live(pid, id)?;
        self.authenticated = true;
        Ok(())
    }
    pub(super) fn observe_exit(&mut self) -> io::Result<bool> {
        if self.helper.observe_exit()?.is_some() {
            self.exited = true;
        }
        Ok(self.exited)
    }
    pub(super) fn close_control(&mut self) -> io::Result<()> {
        if let Some(control) = self.control.as_mut() {
            control.close_once()?;
        }
        Ok(())
    }
    pub(super) fn close_listener(&mut self) -> io::Result<()> {
        if let Some(listener) = self.listener.as_mut() {
            listener.close_once()?;
        }
        Ok(())
    }
    pub(super) fn wait_authorization(&mut self) -> io::Result<()> {
        if self.waited.is_none() {
            if let Some(child) = self.child.as_mut() {
                self.waited = Some(child.wait()?);
            }
        }
        Ok(())
    }
    pub(super) fn closed(&self) -> bool {
        (!self.attempted
            || self.child.is_none()
            || ((self.exited || self.cancelled) && self.waited.is_some()))
            && [&self.listener, &self.control, &self.auth_output]
                .into_iter()
                .flatten()
                .all(Original::closed)
            && self.helper.event_closed()
    }
}
pub(super) fn set_flags(fd: RawFd) -> io::Result<()> {
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
    if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
        return Err(io::Error::last_os_error());
    }
    let enabled = 1_i32;
    if unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_NOSIGPIPE,
            (&enabled as *const i32).cast(),
            std::mem::size_of_val(&enabled) as _,
        )
    } < 0
    {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}
pub(super) const OWNED_SCRIPT: &str = r#"
on run argv
    if (count of argv) is 0 then error "missing privileged command"
    set commandText to quoted form of (item 1 of argv)
    set nonceText to item (count of argv) of argv
    repeat with argumentIndex from 2 to ((count of argv) - 1)
        set commandText to commandText & " " & quoted form of (item argumentIndex of argv)
    end repeat
    try
        do shell script commandText with administrator privileges
        return "zero.utun.completed." & nonceText
    on error errorText number errorNumber
        if errorNumber is -128 then return "zero.utun.cancelled." & nonceText
        error errorText number errorNumber
    end try
end run
"#;
