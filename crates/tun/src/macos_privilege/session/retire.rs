//! Same actual helper duty retains late native close progress until completion.
use super::*;
impl MacosHelperOwner {
    /// Runs only inside the existing, actually registered initial helper duty.
    /// Healthy route work retains the same helper; a real helper exit can let
    /// this actor join independently of the long-lived route worker.
    pub fn wait_initial(&self) -> io::Result<()> {
        loop {
            let wait = self
                .0
                .wait
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            let (ready, event) = {
                let mut epoch = self.0.epochs[0].try_lock().ok();
                match epoch.as_mut() {
                    Some(epoch) => {
                        let ready = match epoch.native.observe_exit() {
                            Ok(ready) => ready,
                            Err(source) => {
                                self.0.retain(0, Phase::HelperExit, source);
                                false
                            }
                        };
                        (ready, epoch.native.helper.event_fd())
                    }
                    None => (false, None),
                }
            };
            if ready
                || (self.0.stopped.load(Ordering::Acquire)
                    && !self.0.route_held.load(Ordering::Acquire))
            {
                if ready && !self.0.stopped.load(Ordering::Acquire) {
                    self.0.retain(
                        0,
                        Phase::Shutdown,
                        io::Error::other("actual helper exited before Source requested stop"),
                    );
                }
                drop(wait);
                break;
            }
            let wake = self
                .0
                .wake_io
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .fd();
            if let (Some(event), Some(wake)) = (event, wake) {
                drop(wait);
                let mut fds = [
                    libc::pollfd {
                        fd: event,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: wake,
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                let count = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, -1) };
                if count < 0 {
                    let source = io::Error::last_os_error();
                    if source.kind() != io::ErrorKind::Interrupted {
                        self.0.retain(0, Phase::HelperExit, source);
                    }
                }
                self.0
                    .wake_io
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .drain();
            } else {
                drop(
                    self.0
                        .wake
                        .wait(wait)
                        .unwrap_or_else(|error| error.into_inner()),
                );
            }
        }
        let result = loop {
            let result = self.finish_initial();
            self.0.close_actor_wake();
            if self.0.require_closed().is_ok() {
                break result;
            }
            if let Err(source) = result {
                self.0.retain(0, Phase::Shutdown, source);
            }
            // The same registered body owns the late close continuation.
            // A failed observer cannot strand it in an exited task or invent
            // another cleanup task/worker just to obtain a later receipt.
            self.wait_original_progress();
        };
        match self.failure() {
            Some(source) => Err(io::Error::other(source)),
            None => result,
        }
    }
    fn finish_initial(&self) -> io::Result<()> {
        let _notifications = self.0.memory.defer_notifications();
        let mut epoch = self.0.epochs[0]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if epoch.native.child.is_some() && epoch.native.control.is_none() && !epoch.native.cancelled
        {
            // The failed observer did not destroy its authorization/accept
            // continuation. A late connection belongs to this same launch,
            // not another helper attempt. Unknown without a late peer stays
            // genuinely owned and keeps this actual blocking duty registered.
            loop {
                match epoch.connect() {
                    Ok(()) => break,
                    Err(source) => {
                        self.0.retain(0, Phase::Shutdown, source);
                        if epoch.native.cancelled {
                            break;
                        }
                        let Some(listener) = epoch
                            .native
                            .listener
                            .as_ref()
                            .filter(|original| original.usable())
                            .map(Original::fd)
                        else {
                            return Err(io::Error::other(
                                "failed launch has no observable original continuation",
                            ));
                        };
                        let mut fd = libc::pollfd {
                            fd: listener,
                            events: libc::POLLIN,
                            revents: 0,
                        };
                        loop {
                            let count = unsafe { libc::poll(&mut fd, 1, -1) };
                            if count >= 0 {
                                break;
                            }
                            let source = io::Error::last_os_error();
                            if source.kind() != io::ErrorKind::Interrupted {
                                return Err(source);
                            }
                        }
                    }
                }
            }
        }
        if epoch.native.child.is_some()
            && !epoch.native.cancelled
            && !epoch.native.observe_exit()?
        {
            if epoch.rpc.pending() {
                match epoch.drive_rpc().and_then(|()| epoch.rpc.take_response()) {
                    Ok(response) => drop(response),
                    Err(source) => {
                        self.0.retain(0, Phase::ToolProgress, source);
                        // Stop reading a reply that cannot fit its actual
                        // revoked/full storage. Closing this exact control
                        // requests helper exit; the independent tool event,
                        // transfer and actual helper NOTE_EXIT still must seal.
                        if let Err(source) = epoch.native.close_control() {
                            self.0.retain(0, Phase::Control, source);
                        }
                        while !epoch.native.observe_exit()? {
                            epoch.wait_io(libc::POLLIN)?;
                        }
                        return self.0.finish_epoch(0, &mut epoch);
                    }
                }
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
