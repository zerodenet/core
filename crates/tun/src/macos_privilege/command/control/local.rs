//! Only the original worker reaps. Stop can signal its exact unreaped Child.
use super::*;
pub(super) struct Local {
    child: Option<Child>,
    waited: Option<ExitStatus>,
    begun: bool,
    spawn_returned: bool,
    cleanup: bool,
    signalled: bool,
}
impl Local {
    pub(super) fn new() -> Self {
        Self {
            child: None,
            waited: None,
            begun: false,
            spawn_returned: false,
            cleanup: false,
            signalled: false,
        }
    }
}
impl CommandControl {
    pub(in crate::macos_privilege) fn begin_local(
        &self,
        mode: &crate::macos_privilege::session::CommandMode,
    ) -> io::Result<()> {
        let cleanup = self.checked_mode(mode)?;
        let mut local = self
            .local
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if local.child.is_some() && local.waited.is_none() {
            return Err(io::Error::other("same local Child has not been reaped"));
        }
        // Reject before creating any pending spawn responsibility. A race
        // after this check is handled by install_child on the same control.
        if self.stopped.load(Ordering::Acquire) && !cleanup {
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        *local = Local::new();
        local.begun = true;
        local.cleanup = cleanup;
        Ok(())
    }
    pub(in crate::macos_privilege) fn install_child(
        &self,
        child: Child,
    ) -> Result<(), (io::Error, Child)> {
        {
            let mut local = self
                .local
                .lock()
                .unwrap_or_else(|source| source.into_inner());
            if !local.begun || local.child.is_some() {
                return Err((
                    io::Error::other("actual local Child slot already installed"),
                    child,
                ));
            }
            local.child = Some(child);
            local.spawn_returned = true;
        }
        self.signal_local();
        Ok(())
    }
    // Only the actual std::spawn Err or a proven failure BEFORE invoking it
    // reaches this boundary. Not an exit receipt: FD/worker debts stay separate.
    pub(in crate::macos_privilege) fn observe_no_returned_child(&self) {
        self.local
            .lock()
            .unwrap_or_else(|source| source.into_inner())
            .spawn_returned = true;
    }
    pub(in crate::macos_privilege) fn require_no_live_child(&self) -> io::Result<()> {
        let local = self
            .local
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if local.begun
            && (!local.spawn_returned || (local.child.is_some() && local.waited.is_none()))
        {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        Ok(())
    }
    fn signal_local(&self) {
        let mut local = self
            .local
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if !self.stopped.load(Ordering::Acquire)
            || local.cleanup
            || local.signalled
            || local.waited.is_some()
        {
            return;
        }
        let Some(child) = local.child.as_mut() else {
            return;
        };
        // The same mutex excludes the unique final std::Child::wait/reap. A
        // PID cannot be recycled between deciding and signalling this Child.
        let result = child.kill();
        local.signalled = true;
        if let Err(source) = result {
            let _ = self.local_fault.set(source);
        }
    }
    pub(in crate::macos_privilege) fn local_cancelled(&self) -> bool {
        let local = self
            .local
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        self.stopped.load(Ordering::Acquire) && !local.cleanup
    }
    pub(in crate::macos_privilege) fn local_reaped(&self) -> bool {
        let local = self
            .local
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        local.waited.is_some()
    }
    #[cfg(test)]
    pub(in crate::macos_privilege) fn test_child_id(&self) -> Option<u32> {
        self.local.lock().unwrap().child.as_ref().map(Child::id)
    }
    pub(in crate::macos_privilege) fn local_has_child(&self) -> bool {
        self.local
            .lock()
            .unwrap_or_else(|source| source.into_inner())
            .child
            .is_some()
    }
    pub(in crate::macos_privilege) fn wait_local(&self) -> io::Result<ExitStatus> {
        let pid = {
            let local = self
                .local
                .lock()
                .unwrap_or_else(|source| source.into_inner());
            if let Some(status) = local.waited {
                return Ok(status);
            }
            local
                .child
                .as_ref()
                .ok_or_else(|| io::Error::other("std::spawn did not return this local Child"))?
                .id()
        };
        // WNOWAIT observes, never reaps. The actual Child and unreaped PID stay
        // owned during this blocking syscall, with NO control mutex held.
        loop {
            let mut info = unsafe { std::mem::zeroed::<libc::siginfo_t>() };
            if unsafe { libc::waitid(libc::P_PID, pid, &mut info, libc::WEXITED | libc::WNOWAIT) }
                == 0
            {
                break;
            }
            let source = io::Error::last_os_error();
            if source.kind() != io::ErrorKind::Interrupted {
                return Err(source);
            }
        }
        let mut local = self
            .local
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        // The sole worker consumes std::wait only AFTER the real terminal
        // observation. This no-longer-blocking reap is serialized with kill.
        let status = local.child.as_mut().unwrap().wait()?;
        local.waited = Some(status);
        Ok(status)
    }
}
