//! One fixed command at a time, supervised by its original Proxy RPC worker.
//! This does not claim a general process tree or create another monitor task.
use super::{process::ProcessExit, transfer::Original, HelperProgram};
use std::{
    io,
    os::fd::{AsRawFd, IntoRawFd, RawFd},
    os::unix::net::UnixStream,
};
mod child;
pub(super) use child::{run, run_query, ChildOriginal};
pub(in crate::macos_privilege) enum Query {
    Route(Vec<String>),
    RouteTable(bool),
    SystemDns,
}
mod progress;

pub(super) struct Supervisor {
    local: Option<Original>,
    remote: Option<Original>,
    nonce: Option<u64>,
    child: ProcessExit,
    report: [u8; 16],
    read: usize,
    ack: usize,
    live: [u8; 8],
    live_read: usize,
}
impl Supervisor {
    pub(super) fn new() -> Self {
        Self {
            local: None,
            remote: None,
            nonce: None,
            child: ProcessExit::new(),
            report: [0; 16],
            read: 0,
            ack: 0,
            live: [0; 8],
            live_read: 0,
        }
    }
    /// Both descriptors enter the installed Session before any fcntl or IPC.
    pub(super) fn prepare(&mut self, identity: u64) -> io::Result<()> {
        if self.local.is_some() || self.remote.is_some() {
            return Err(io::Error::other("same tool supervisor already attempted"));
        }
        let (local, remote) = UnixStream::pair()?;
        self.local = Some(Original::new(local.into_raw_fd(), identity));
        self.remote = Some(Original::new(remote.into_raw_fd(), identity));
        for original in [&self.local, &self.remote].into_iter().flatten() {
            let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFD) };
            if flags < 0
                || unsafe { libc::fcntl(original.fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) }
                    < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let enabled = 1_i32;
        for original in [&self.local, &self.remote].into_iter().flatten() {
            if unsafe {
                libc::setsockopt(
                    original.fd(),
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    (&enabled as *const i32).cast(),
                    std::mem::size_of_val(&enabled) as _,
                )
            } < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let fd = self.local.as_ref().unwrap().fd();
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) } < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    pub(super) fn remote_fd(&self) -> io::Result<RawFd> {
        self.remote
            .as_ref()
            .filter(|original| original.usable())
            .map(Original::fd)
            .ok_or_else(|| io::Error::other("supervisor endpoint is unavailable"))
    }
    /// The helper retained its received endpoint before sending this ACK.
    pub(super) fn finish_transfer(&mut self, stream: &mut UnixStream) -> io::Result<()> {
        self.remote
            .as_mut()
            .ok_or_else(|| io::Error::other("supervisor transfer missing"))?
            .finish(stream)
    }
    pub(super) fn begin(&mut self, nonce: u64) -> io::Result<()> {
        if self.nonce.is_some() {
            self.child.take_completed()?;
        }
        self.nonce = Some(nonce);
        self.report = [0; 16];
        self.read = 0;
        self.ack = 0;
        self.live = [0; 8];
        self.live_read = 0;
        Ok(())
    }
    pub(super) fn poll_fd(&self) -> Option<RawFd> {
        self.local
            .as_ref()
            .filter(|original| original.usable())
            .map(Original::fd)
    }
    pub(super) fn event_fd(&self) -> Option<RawFd> {
        self.child.event_fd()
    }
    pub(super) fn active(&self) -> bool {
        self.nonce.is_some()
    }
    pub(super) fn completed(&mut self) -> io::Result<bool> {
        if self.nonce.is_none() {
            return Ok(true);
        }
        self.advance()?;
        Ok(self.child.observe_exit()?.is_some())
    }
    /// A known incomplete request has not reached the helper parser. This
    /// narrow fact is supplied by the same retained RPC byte continuation.
    pub(super) fn cancel_unsubmitted(&mut self, nonce: u64) -> io::Result<()> {
        if self.nonce != Some(nonce) || self.read != 0 || self.child.event_fd().is_some() {
            return Err(io::Error::other("cannot erase a submitted tool original"));
        }
        self.nonce = None;
        Ok(())
    }
    pub(super) fn close(&mut self) -> io::Result<()> {
        let mut failure = None;
        if self.active() && !self.completed()? {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        if let Err(source) = self.child.close_event() {
            failure.get_or_insert(source);
        }
        for original in [&mut self.local, &mut self.remote].into_iter().flatten() {
            if let Err(source) = original.close_once() {
                failure.get_or_insert(source);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    pub(super) fn closed(&self) -> bool {
        self.child.event_closed()
            && [&self.local, &self.remote]
                .into_iter()
                .flatten()
                .all(Original::closed)
    }
}

#[cfg(test)]
mod tests;
