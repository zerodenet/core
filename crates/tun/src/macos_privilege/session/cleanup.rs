//! A late receipt progresses the same originals; no close action is recreated.
use super::*;
impl Session {
    pub(super) fn require_closed(&self) -> io::Result<()> {
        for original in &self.epochs {
            let epoch = original
                .try_lock()
                .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
            epoch.transfer.require_closed()?;
            if !epoch.native.closed() || !epoch.tools.closed() || !epoch.local_tool.reusable() {
                return Err(io::Error::other(
                    "same helper/tool original has no complete close receipt",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn prepare_cleanup(&self) -> io::Result<()> {
        if self.current.load(Ordering::Acquire) == 1 {
            return Ok(());
        }
        let mut old = self.epochs[0]
            .try_lock()
            .map_err(|_| io::Error::from(io::ErrorKind::WouldBlock))?;
        if !old.native.attempted || unsafe { libc::geteuid() } == 0 {
            return Ok(());
        }
        if !old.native.observe_exit()? {
            return Ok(());
        } // restore through this still healthy helper.
        self.finish_epoch(0, &mut old)?;
        if !self.initial_driver_returned.load(Ordering::Acquire) {
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
    fn finish_epoch(&self, index: usize, epoch: &mut Epoch) -> io::Result<()> {
        let exited = match epoch.native.observe_exit() {
            Ok(exited) => exited,
            Err(source) => {
                self.retain(index, Phase::HelperExit, source);
                false
            }
        };
        if epoch.native.child.is_some() && !exited {
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
        if let Err(source) = epoch.native.helper.close_event() {
            self.retain(index, Phase::Event, source);
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
impl MacosHelperOwner {
    /// Runs only inside the existing, actually registered initial helper duty.
    /// Healthy route work retains the same helper; a real helper exit can let
    /// this actor join independently of the long-lived route worker.
    pub fn wait_initial(&self) -> io::Result<()> {
        loop {
            let mut wait = self
                .0
                .wait
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let ready = {
                let mut epoch = self.0.epochs[0].try_lock().ok();
                epoch
                    .as_mut()
                    .is_some_and(|epoch| epoch.native.observe_exit().unwrap_or(false))
            };
            if ready
                || (self.0.stopped.load(Ordering::Acquire)
                    && !self.0.route_held.load(Ordering::Acquire))
            {
                break;
            }
            wait = self
                .0
                .wake
                .wait(wait)
                .unwrap_or_else(|error| error.into_inner());
            drop(wait);
        }
        self.finish_initial()
    }
    fn finish_initial(&self) -> io::Result<()> {
        let mut epoch = self.0.epochs[0]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if epoch.native.child.is_some() && !epoch.native.observe_exit()? {
            if epoch.rpc.pending() {
                epoch.drive_rpc()?;
                drop(epoch.rpc.take_response()?);
            }
            if !epoch.shutdown_started {
                epoch.rpc.begin(&HelperRequest::Shutdown, None)?;
                epoch.shutdown_started = true;
            }
            while !epoch.rpc.submitted() {
                let fd = epoch.native.control_fd()?;
                epoch.rpc.write_request(fd)?;
                if !epoch.rpc.submitted() {
                    epoch.wait_io(libc::POLLOUT)?;
                }
            }
            if let Err(source) = epoch.native.close_control() {
                self.0.retain(0, Phase::Control, source);
            }
            while !epoch.native.observe_exit()? {
                epoch.wait_io(libc::POLLIN)?;
            }
        }
        self.0.finish_epoch(0, &mut epoch)?;
        match self.failure() {
            Some(source) => Err(io::Error::other(source)),
            None => Ok(()),
        }
    }
}
