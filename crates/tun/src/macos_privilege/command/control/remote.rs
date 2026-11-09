//! Borrowed same supervisor endpoint; its original owner serializes close here.
use super::*;
use std::os::fd::RawFd;
pub(in crate::macos_privilege) const ACK: u8 = 0xa1;
pub(in crate::macos_privilege) const CANCEL: u8 = 0xc1;
pub(in crate::macos_privilege) const EXIT: u8 = 0xe1;
pub(in crate::macos_privilege) const MESSAGE: usize = 9;
pub(super) struct Remote {
    fd: Option<RawFd>,
    nonce: Option<u64>,
    cleanup: bool,
    registered: bool,
    ack: usize,
    cancel: usize,
    previous_cancel: Option<(u64, usize)>,
    previous_exit: Option<(u64, usize)>,
    exited: bool,
    exit: usize,
}
impl Remote {
    pub(super) fn new() -> Self {
        Self {
            fd: None,
            nonce: None,
            cleanup: false,
            registered: false,
            ack: 0,
            cancel: 0,
            previous_cancel: None,
            previous_exit: None,
            exited: false,
            exit: 0,
        }
    }
}
impl CommandControl {
    pub(in crate::macos_privilege) fn bind_remote(&self, fd: RawFd) -> io::Result<()> {
        let mut remote = self
            .remote
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if remote.fd.is_some() {
            return Err(io::Error::other("Source supervisor original already bound"));
        }
        remote.fd = Some(fd);
        Ok(())
    }
    pub(in crate::macos_privilege) fn begin_remote(
        &self,
        nonce: u64,
        mode: &crate::macos_privilege::session::CommandMode,
    ) -> io::Result<()> {
        let cleanup = self.checked_mode(mode)?;
        let mut remote = self
            .remote
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if self.stopped.load(Ordering::Acquire) && !cleanup {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        // Check both finite tails before changing either: a failed begin
        // must not copy an offset and then later replay that same suffix.
        let keep_cancel = remote.registered
            && !remote.cleanup
            && self.stopped.load(Ordering::Acquire)
            && (!remote.exited || remote.cancel != 0)
            && remote.cancel < MESSAGE;
        let keep_exit = remote.exited && remote.cancel == 0 && remote.exit < MESSAGE;
        if (keep_cancel && (remote.ack != MESSAGE || remote.previous_cancel.is_some()))
            || (keep_exit && (remote.ack != MESSAGE || remote.previous_exit.is_some()))
        {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        if keep_cancel {
            remote.previous_cancel = Some((remote.nonce.unwrap(), remote.cancel));
        }
        if keep_exit {
            remote.previous_exit = Some((remote.nonce.unwrap(), remote.exit));
        }
        remote.nonce = Some(nonce);
        remote.cleanup = cleanup;
        remote.registered = false;
        remote.ack = 0;
        remote.cancel = 0;
        remote.exited = false;
        remote.exit = 0;
        Ok(())
    }
    pub(super) fn signal_remote(&self) {
        let mut remote = self
            .remote
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if let Err(source) = self.flush_remote(&mut remote) {
            let _ = self.remote_fault.set(source);
        }
    }
    fn flush_remote(&self, remote: &mut Remote) -> io::Result<()> {
        if let Some((nonce, offset)) = &mut remote.previous_cancel {
            let Some(fd) = remote.fd else {
                return Ok(());
            };
            let mut message = [0; MESSAGE];
            message[0] = CANCEL;
            message[1..].copy_from_slice(&nonce.to_be_bytes());
            if !write(fd, &message, offset)? {
                return Ok(());
            }
            remote.previous_cancel = None;
        }
        if let Some((nonce, offset)) = &mut remote.previous_exit {
            let Some(fd) = remote.fd else {
                return Ok(());
            };
            let mut message = [0; MESSAGE];
            message[0] = EXIT;
            message[1..].copy_from_slice(&nonce.to_be_bytes());
            if !write(fd, &message, offset)? {
                return Ok(());
            }
            remote.previous_exit = None;
        }
        if !remote.registered {
            return Ok(());
        }
        let Some(nonce) = remote.nonce else {
            return Ok(());
        };
        let Some(fd) = remote.fd else {
            return Ok(());
        };
        let mut message = [0; MESSAGE];
        message[1..].copy_from_slice(&nonce.to_be_bytes());
        // ACK only after actual NOTE_EXIT registration. A partial ACK is always
        // completed contiguously before cancel; stop cannot release pre_exec
        // before the actual kernel registration or corrupt its framing.
        message[0] = ACK;
        if !write(fd, &message, &mut remote.ack)? {
            return Ok(());
        }
        // A real terminal observation needs no NEW cancellation. In
        // particular, never insert CANCEL into a partially written EXIT.
        // An already started CANCEL must still finish its exact suffix.
        if self.stopped.load(Ordering::Acquire)
            && !remote.cleanup
            && (!remote.exited || remote.cancel != 0)
        {
            message[0] = CANCEL;
            if !write(fd, &message, &mut remote.cancel)? {
                return Ok(());
            }
        }
        // A completed CANCEL makes the helper enter its own true wait;
        // its EXIT offset remains zero because no EXIT bytes were emitted.
        if remote.exited && remote.cancel == 0 {
            message[0] = EXIT;
            write(fd, &message, &mut remote.exit)?;
        }
        Ok(())
    }
    pub(in crate::macos_privilege) fn acknowledge_remote(&self, nonce: u64) -> io::Result<bool> {
        let mut remote = self
            .remote
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if remote.nonce != Some(nonce) {
            return Err(io::Error::other(
                "NOTE_EXIT registration belongs to another actual command",
            ));
        }
        remote.registered = true;
        self.flush_remote(&mut remote)?;
        Ok(remote.ack == MESSAGE)
    }
    // Only Supervisor.advance calls this after its own original NOTE_EXIT.
    // The wire notification grants no FD/Scope close or Host ACK.
    pub(in crate::macos_privilege) fn observe_remote_exit(&self, nonce: u64) -> io::Result<()> {
        let mut remote = self
            .remote
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if remote.nonce != Some(nonce) || !remote.registered {
            return Err(io::Error::other(
                "terminal observation belongs to another command",
            ));
        }
        remote.exited = true;
        self.flush_remote(&mut remote)
    }
    pub(in crate::macos_privilege) fn remote_pending_write(&self) -> bool {
        let remote = self
            .remote
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        remote.previous_cancel.is_some()
            || remote.previous_exit.is_some()
            || (remote.registered
                && (remote.ack < MESSAGE
                    || (self.stopped.load(Ordering::Acquire)
                        && !remote.cleanup
                        && (!remote.exited || remote.cancel != 0)
                        && remote.cancel < MESSAGE)
                    || (remote.exited && remote.cancel == 0 && remote.exit < MESSAGE)))
    }
    pub(in crate::macos_privilege) fn unbind_remote(&self) -> io::Result<()> {
        let mut remote = self
            .remote
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        remote.fd = None;
        Ok(())
    }
}
fn write(fd: RawFd, bytes: &[u8; MESSAGE], offset: &mut usize) -> io::Result<bool> {
    while *offset < bytes.len() {
        let count =
            unsafe { libc::write(fd, bytes[*offset..].as_ptr().cast(), bytes.len() - *offset) };
        if count > 0 {
            *offset += count as usize;
            continue;
        }
        if count == 0 {
            return Err(io::Error::from_raw_os_error(libc::EPIPE));
        }
        let source = io::Error::last_os_error();
        match source.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Ok(false),
            _ => return Err(source),
        }
    }
    Ok(true)
}
