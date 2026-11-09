//! One retained connection/RPC continuation; no replay of submitted prefixes.
use super::*;
impl Epoch {
    pub(super) fn connect(&mut self) -> io::Result<()> {
        if self.stage == Stage::Cold {
            let began = self.native.begin(self.id);
            // A late fallible step after spawn must not replay authorization
            // or lose the already retained accept/Child continuation.
            if began.is_ok() || self.native.child.is_some() {
                self.stage = Stage::Accept;
            }
            began?;
        }
        if self.stage == Stage::Accept {
            self.native.accept(self.id)?;
            self.rpc
                .begin(&HelperRequest::Hello { nonce: self.id }, None)?;
            self.stage = Stage::Hello;
        }
        if self.stage == Stage::Hello {
            self.drive_rpc()?;
            let response = self.rpc.take_response()?;
            self.native.authenticate(self.id, &response)?;
            self.tools.prepare(self.id)?;
            self.rpc.begin(
                &HelperRequest::InstallSupervisor {
                    transfer: self.id,
                    request_capacity: self.rpc.request_capacity(),
                },
                None,
            )?;
            self.stage = Stage::SupervisorRequest;
        }
        if self.stage == Stage::SupervisorRequest {
            let fd = self.native.control_fd()?;
            while !self.rpc.submitted() {
                self.rpc.write_request(fd)?;
                if !self.rpc.submitted() {
                    self.wait_io(libc::POLLOUT)?;
                }
            }
            self.stage = Stage::SupervisorFd;
        }
        if self.stage == Stage::SupervisorFd {
            loop {
                match super::super::send_fd(self.native.control_fd()?, self.tools.remote_fd()?) {
                    Ok(()) => break,
                    Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                        self.wait_io(libc::POLLOUT)?
                    }
                    Err(source) => return Err(source),
                }
            }
            self.stage = Stage::SupervisorAck;
        }
        if self.stage == Stage::SupervisorAck {
            self.drive_rpc()?;
            if self.rpc.pending() {
                let response = self.rpc.take_response()?;
                if !response.success {
                    return Err(io::Error::other(
                        "same helper did not retain its supervisor endpoint",
                    ));
                }
            }
            let fd = self.native.control_fd()?;
            let mut stream = unsafe { std::mem::ManuallyDrop::new(UnixStream::from_raw_fd(fd)) };
            loop {
                match self.tools.finish_transfer(&mut stream) {
                    Ok(()) => break,
                    Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                        self.wait_io(libc::POLLIN | libc::POLLOUT)?
                    }
                    Err(source) => return Err(source),
                }
            }
            self.stage = Stage::Ready;
        }
        Ok(())
    }
    pub(super) fn drive_rpc(&mut self) -> io::Result<()> {
        let fd = self.native.control_fd()?;
        loop {
            if self.rpc.progress(fd, &mut self.tools)? {
                return Ok(());
            }
            self.wait_io(if self.rpc.submitted() {
                libc::POLLIN
            } else {
                libc::POLLOUT
            })?;
        }
    }
    pub(super) fn wait_io(&self, events: i16) -> io::Result<()> {
        let mut fds = [
            libc::pollfd {
                fd: self.native.control_fd().unwrap_or(-1),
                events,
                revents: 0,
            },
            libc::pollfd {
                fd: self.tools.poll_fd().unwrap_or(-1),
                events: self.tools.poll_events(),
                revents: 0,
            },
            libc::pollfd {
                fd: self.tools.event_fd().unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.native.helper.event_fd().unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        loop {
            let count = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, -1) };
            if count >= 0 {
                return Ok(());
            }
            let source = io::Error::last_os_error();
            if source.kind() != io::ErrorKind::Interrupted {
                return Err(source);
            }
        }
    }
}
