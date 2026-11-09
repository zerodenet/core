//! Exact Source-owned command cancellation, never task/native-close evidence.
use std::{
    io,
    process::{Child, ExitStatus},
    sync::{
        atomic::{AtomicBool, Ordering},
        Mutex, OnceLock,
    },
};
mod local;
mod remote;
use local::Local;
use remote::Remote;
pub(in crate::macos_privilege) use remote::{ACK, CANCEL, EXIT, MESSAGE};
pub(in crate::macos_privilege) struct CommandControl {
    stopped: AtomicBool,
    local: Mutex<Local>,
    remote: Mutex<Remote>,
    local_fault: OnceLock<io::Error>,
    remote_fault: OnceLock<io::Error>,
}
impl CommandControl {
    pub(in crate::macos_privilege) fn new() -> Self {
        Self {
            stopped: AtomicBool::new(false),
            local: Mutex::new(Local::new()),
            remote: Mutex::new(Remote::new()),
            local_fault: OnceLock::new(),
            remote_fault: OnceLock::new(),
        }
    }
    pub(in crate::macos_privilege) fn request_stop(&self) {
        self.stopped.store(true, Ordering::Release);
        self.signal_local();
        self.signal_remote();
    }
    pub(in crate::macos_privilege) fn any_fault(&self) -> bool {
        self.local_fault.get().is_some() || self.remote_fault.get().is_some()
    }
    pub(in crate::macos_privilege) fn first_fault(&self) -> Option<&io::Error> {
        self.local_fault.get().or_else(|| self.remote_fault.get())
    }
    fn checked_mode(
        &self,
        mode: &crate::macos_privilege::session::CommandMode,
    ) -> io::Result<bool> {
        if !mode.matches(self) {
            return Err(io::Error::other(
                "command mode belongs to another actual Source control",
            ));
        }
        let cleanup = mode.is_cleanup();
        if self.stopped.load(Ordering::Acquire) && !cleanup {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        Ok(cleanup)
    }
}
impl std::fmt::Debug for CommandControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for CommandControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(source) = self.local_fault.get() {
            write!(f, "original local Child cancel: {source}")?;
        }
        if let Some(source) = self.remote_fault.get() {
            write!(f, " original supervisor cancel: {source}")?;
        }
        Ok(())
    }
}
impl std::error::Error for CommandControl {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.first_fault().map(|source| source as _)
    }
}

