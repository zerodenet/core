use super::*;
impl Server {
    pub(super) fn install_supervisor(
        &mut self,
        stream: &mut UnixStream,
        id: u64,
    ) -> io::Result<()> {
        if self.supervisor.is_some() || self.transfer.is_some() {
            return Err(io::Error::other("same helper supervisor already attempted"));
        }
        self.transfer = Some(Transfer::new(id));
        let transfer = self.transfer.as_ref().unwrap();
        transfer.submit_create();
        let fd = transfer.receive_fd(stream.as_raw_fd())?;
        self.supervisor = Some(Original::new(fd, id));
        transfer.claim_received(fd)?;
        // Store actual unique endpoint and close-on-exec before its ACK. The
        // helper may subsequently inherit it only across pre_exec until exec.
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if flags < 0 || unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        write_helper_response(
            stream,
            &HelperResponse {
                success: true,
                code: 0,
                stdout: Vec::new(),
                stderr: Vec::new(),
            },
        )?;
        transfer.confirm(stream, fd)
    }
}
