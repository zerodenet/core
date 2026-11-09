//! Passive original-event wait for the existing initial helper duty.
use super::*;
impl MacosHelperOwner {
    pub(super) fn wait_original_progress(&self, index: usize, initial: bool) {
        let mut fds = {
            let epoch = self.0.epochs[index]
                .lock()
                .unwrap_or_else(|source| source.into_inner());
            [
                epoch.native.helper.event_fd(),
                epoch.tools.event_fd(),
                epoch.tools.poll_fd(),
                epoch
                    .native
                    .listener
                    .as_ref()
                    .filter(|original| original.usable())
                    .map(Original::fd),
                epoch
                    .native
                    .auth_output
                    .as_ref()
                    .filter(|original| original.usable())
                    .map(Original::fd),
            ]
            .map(|fd| libc::pollfd {
                fd: fd.unwrap_or(-1),
                events: libc::POLLIN,
                revents: 0,
            })
        };
        if fds.iter().all(|fd| fd.fd < 0) {
            self.0.wait_signal(initial);
            return;
        }
        loop {
            if unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as _, -1) } >= 0 {
                return;
            }
            let source = io::Error::last_os_error();
            if source.kind() != io::ErrorKind::Interrupted {
                self.0.retain(0, Phase::Shutdown, source);
                return;
            }
        }
    }
}
