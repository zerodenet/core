//! The same parent installs Raw before acknowledging the helper's FD.
use super::*;
impl MacosHelperOwner {
    pub(crate) fn utun_transfer_settled(&self) -> bool {
        self.0.epochs[0]
            .try_lock()
            .is_ok_and(|epoch| epoch.transfer.require_closed().is_ok())
    }

    pub(crate) fn acquire_into(
        &self,
        parent: &std::sync::Arc<crate::AdoptedTunOwner>,
        name: Option<&str>,
    ) -> io::Result<()> {
        if self.0.stopped.load(Ordering::Acquire) || self.0.current.load(Ordering::Acquire) != 0 {
            return Err(io::Error::other(
                "retiring helper cannot create another utun original",
            ));
        }
        let mut epoch = self.0.epochs[0]
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        epoch.connect()?;
        if epoch.create_attempted {
            return Err(io::Error::other("same helper utun request cannot replay"));
        }
        epoch.create_attempted = true;
        epoch.transfer.submit_create();
        let id = epoch.id;
        epoch.rpc.begin(
            &HelperRequest::OwnedCreateUtun {
                name: name.map(str::to_owned),
                transfer: id,
            },
            None,
        )?;
        epoch.drive_rpc()?;
        let response = epoch.rpc.take_response()?;
        if response.stdout.len() != 13 || response.stdout[..8] != id.to_be_bytes() {
            return Err(io::Error::other(
                "owned helper acquisition has no matching original disposition",
            ));
        }
        let close_errno = i32::from_be_bytes(response.stdout[9..13].try_into().unwrap());
        match response.stdout[8] {
            0 if response.success => {} // Acquired; helper still owes its ACK close.
            1 if !response.success => {
                epoch.transfer.observe_no_native();
                return Err(create_error(&response));
            }
            2 if !response.success => {
                epoch.transfer.observe_remote_close(0);
                return Err(create_error(&response));
            }
            3 if !response.success && close_errno != 0 => {
                epoch.transfer.observe_remote_close(close_errno);
                self.0.retain(
                    0,
                    Phase::Unclaimed,
                    io::Error::from_raw_os_error(close_errno),
                );
                return Err(create_error(&response));
            }
            _ => return Err(io::Error::other("owned helper disposition is invalid")),
        }
        let fd = loop {
            match epoch.transfer.receive_fd(epoch.native.control_fd()?) {
                Ok(fd) => break fd,
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                    epoch.wait_io(libc::POLLIN)?
                }
                Err(source) => return Err(source),
            }
        };
        // No FD, native/source identity or close authority comes from an id.
        // This exact parent checks its original helper owner and Acquiring
        // state, then stores OwnedFd+identity before claim and ACK can run.
        parent.retain_macos_transferred(self, fd)?;
        epoch.transfer.claim_received(fd)?;
        let raw = epoch.native.control_fd()?;
        let mut stream = unsafe { std::mem::ManuallyDrop::new(UnixStream::from_raw_fd(raw)) };
        loop {
            match epoch.transfer.confirm(&mut stream, fd) {
                Ok(()) => return Ok(()),
                Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                    epoch.wait_io(libc::POLLIN | libc::POLLOUT)?
                }
                Err(source) => return Err(source),
            }
        }
    }
}
fn create_error(response: &HelperResponse) -> io::Error {
    if response.code > 0 {
        io::Error::from_raw_os_error(response.code)
    } else {
        io::Error::other("actual helper utun acquisition failed")
    }
}
