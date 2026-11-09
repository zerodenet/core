//! An authenticated helper or one fixed tool, never descendant containment.
use super::transfer::Original;
use std::{io, os::fd::RawFd};

pub(super) struct ProcessExit {
    event: Option<Original>,
    pid: libc::pid_t,
    nonce: u64,
    live: bool,
    registered: bool,
    exited: bool,
}
/// Issued only after the original NOTE_EXIT registration and the same process's
/// post-registration nonce exchange. It says nothing about any descendants.
pub(super) struct ProcessExited {
    pid: libc::pid_t,
    nonce: u64,
}
impl ProcessExited {
    pub(super) fn matches(&self, pid: libc::pid_t, nonce: u64) -> bool {
        self.pid == pid && self.nonce == nonce
    }
}
impl ProcessExit {
    pub(super) fn new() -> Self {
        Self {
            event: None,
            pid: 0,
            nonce: 0,
            live: false,
            registered: false,
            exited: false,
        }
    }
    /// The caller already owns this slot before the first kqueue syscall.
    pub(super) fn register(&mut self, pid: libc::pid_t, nonce: u64) -> io::Result<()> {
        if self.event.is_some() || pid <= 0 {
            return Err(io::Error::other(
                "same process exit original already attempted",
            ));
        }
        self.pid = pid;
        self.nonce = nonce;
        let queue = unsafe { libc::kqueue() };
        if queue < 0 {
            return Err(io::Error::last_os_error());
        }
        self.event = Some(Original::new(queue, nonce));
        let event = self.event.as_ref().unwrap();
        let change = libc::kevent {
            ident: pid as libc::uintptr_t,
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_ONESHOT | libc::EV_RECEIPT,
            fflags: libc::NOTE_EXIT,
            data: 0,
            udata: std::ptr::null_mut(),
        };
        let mut result: libc::kevent = unsafe { std::mem::zeroed() };
        let count =
            unsafe { libc::kevent(event.fd(), &change, 1, &mut result, 1, std::ptr::null()) };
        if count < 0 {
            return Err(io::Error::last_os_error());
        }
        if count != 1 || result.flags & libc::EV_ERROR == 0 {
            return Err(io::Error::other(
                "process registration returned no original receipt",
            ));
        }
        if result.data != 0 {
            return Err(io::Error::from_raw_os_error(result.data as i32));
        }
        self.registered = true;
        Ok(())
    }
    pub(super) fn registered_for(&self, pid: libc::pid_t, nonce: u64) -> bool {
        self.registered
            && self.pid == pid
            && self.nonce == nonce
            && self.event.as_ref().is_some_and(Original::usable)
    }
    /// Only the owning wire reader calls this after reading the matching LIVE
    /// frame. Registering a PID or finding it missing never supplies liveness.
    pub(super) fn observe_live(&mut self, pid: libc::pid_t, nonce: u64) -> io::Result<()> {
        if !self.registered_for(pid, nonce) {
            return Err(io::Error::other("process LIVE belongs to another original"));
        }
        self.live = true;
        Ok(())
    }
    pub(super) fn observe_exit(&mut self) -> io::Result<Option<ProcessExited>> {
        if !self.exited {
            let Some(queue) = self.event.as_ref() else {
                return Ok(None);
            };
            if !queue.usable() {
                return Err(io::Error::other("process event close outcome is unknown"));
            }
            let mut event: libc::kevent = unsafe { std::mem::zeroed() };
            let timeout = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            let count =
                unsafe { libc::kevent(queue.fd(), std::ptr::null(), 0, &mut event, 1, &timeout) };
            if count < 0 {
                return Err(io::Error::last_os_error());
            }
            if count == 1 {
                if event.flags & libc::EV_ERROR != 0 {
                    return Err(io::Error::from_raw_os_error(event.data as i32));
                }
                if event.ident != self.pid as libc::uintptr_t
                    || event.filter != libc::EVFILT_PROC
                    || event.fflags & libc::NOTE_EXIT == 0
                {
                    return Err(io::Error::other(
                        "process exit event mismatches the retained registration",
                    ));
                }
                self.exited = true;
            }
        }
        Ok((self.live && self.exited).then_some(ProcessExited {
            pid: self.pid,
            nonce: self.nonce,
        }))
    }
    pub(super) fn event_fd(&self) -> Option<RawFd> {
        self.event
            .as_ref()
            .filter(|event| event.usable())
            .map(Original::fd)
    }
    pub(super) fn close_event(&mut self) -> io::Result<()> {
        if let Some(event) = self.event.as_mut() {
            event.close_once()?;
        }
        Ok(())
    }
    pub(super) fn event_closed(&self) -> bool {
        self.event.as_ref().is_none_or(Original::closed)
    }
    /// Only a complete former command may reuse its actual one event slot.
    pub(super) fn take_completed(&mut self) -> io::Result<()> {
        if self.event.is_none() {
            return Ok(());
        }
        if self.observe_exit()?.is_none() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.close_event()?;
        *self = Self::new();
        Ok(())
    }
}
#[cfg(test)]
mod tests;
