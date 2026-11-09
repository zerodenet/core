//! A late receipt progresses the same originals; no close action is recreated.
use super::*;
impl Session {
    pub(super) fn require_closed(&self) -> io::Result<()> {
        if !self.wake_io.try_lock().is_ok_and(|wake| wake.closed()) {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        for original in &self.epochs {
            let epoch = original
                .try_lock()
                .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
            epoch.transfer.require_closed()?;
            if !epoch.native.closed()
                || !epoch.tools.closed()
                || !epoch.local_tool.reusable()
                || epoch
                    .local_file
                    .as_ref()
                    .is_some_and(|original| !original.closed())
            {
                return Err(io::Error::other(
                    "same helper/tool original has no complete close receipt",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn prepare_cleanup(&self) -> io::Result<()> {
        let _notifications = self.memory.defer_notifications();
        if self.current.load(Ordering::Acquire) == 1 {
            return Ok(());
        }
        let mut old = self.epochs[0]
            .lock()
            .unwrap_or_else(|source| source.into_inner());
        if !old.native.attempted || unsafe { libc::geteuid() } == 0 {
            return Ok(());
        }
        if !old.native.observe_exit()? {
            return Ok(());
        } // restore through this still healthy helper.
        if !self.initial_driver_returned.load(Ordering::Acquire) {
            // The initial actor may still poll its kqueue. Wake that same
            // actor; never close/recycle its descriptor under the OS wait.
            self.notify();
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        self.finish_epoch(0, &mut old)?;
        if !self
            .wake_io
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .closed()
        {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        old.transfer.require_closed()?;
        if !old.native.closed() || !old.tools.closed() {
            return Err(io::Error::other("old helper remains unknown"));
        }
        // This one cleanup-only epoch is not consumed until all old originals
        // AND the real dedicated helper registration have positively retired.
        self.current
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| io::Error::other("concurrent helper recovery epoch"))?;
        Ok(())
    }
    pub(super) fn finish_epoch(&self, index: usize, epoch: &mut Epoch) -> io::Result<()> {
        let exited = match epoch.native.observe_exit() {
            Ok(exited) => exited,
            Err(source) => {
                self.retain(index, Phase::HelperExit, source);
                false
            }
        };
        if epoch.native.child.is_some() && !exited && !epoch.native.cancelled {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        if exited {
            epoch.transfer.observe_remote_settled();
            if let Err(source) = epoch.rpc.finish_after_helper_exit(&mut epoch.tools) {
                self.retain(index, Phase::ToolProgress, source);
                return Err(io::Error::from(io::ErrorKind::WouldBlock));
            }
        }
        // Capture each original error now, before another independent phase
        // can block in actual Child wait or retain a late pipe continuation.
        if let Err(source) = epoch.native.close_control() {
            self.retain(index, Phase::Control, source);
        }
        if let Err(source) = epoch.native.close_listener() {
            self.retain(index, Phase::Listener, source);
        }
        if let Err(source) = epoch.transfer.close_unclaimed() {
            self.retain(index, Phase::Unclaimed, source);
        }
        if let Err(source) = epoch.tools.close() {
            self.retain(index, Phase::ToolClose, source);
        }
        if let Err(source) = epoch.local_tool.finish_local(self.output_limit) {
            self.retain(index, Phase::ToolClose, source);
        }
        if let Some(original) = epoch.local_file.as_mut() {
            if let Err(source) = original.close_once() {
                self.retain(index, Phase::ToolClose, source);
            }
        }
        if let Err(source) = epoch.native.helper.close_event() {
            self.retain(index, Phase::Event, source);
        }
        if let Err(source) = epoch.native.close_authorization_output() {
            self.retain(index, Phase::AuthorizationOutput, source);
        }
        if let Err(source) = epoch.native.wait_authorization() {
            self.retain(index, Phase::Authorization, source);
        }
        if !epoch.native.closed() || !epoch.tools.closed() {
            return Err(io::Error::from(io::ErrorKind::WouldBlock));
        }
        epoch.transfer.require_closed()
    }
}
