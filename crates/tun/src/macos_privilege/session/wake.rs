//! The initial helper actor waits on its real exit AND this owned stop pipe.
//! No watcher task, queued callback or borrowed/recycled FD is introduced.
use super::*;
pub(super) struct Wake {
    ends: [Option<Original>; 2],
    enabled: bool,
}
impl Wake {
    pub(super) fn new() -> Self {
        Self { ends: [None, None] }
    }
    pub(super) fn prepare(&mut self, id: u64) -> io::Result<()> {
        if self.ends[0].is_some() {
            return if self.enabled {
                Ok(())
            } else {
                Err(io::Error::other(
                    "actual stop-pipe preparation failed before publication",
                ))
            };
        }
        let mut fds = [-1; 2];
        if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
            return Err(io::Error::last_os_error());
        }
        self.ends = [
            Some(Original::new(fds[0], id)),
            Some(Original::new(fds[1], id)),
        ];
        for original in self.ends.iter().flatten() {
            let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFD) };
            if flags < 0
                || unsafe { libc::fcntl(original.fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC) }
                    < 0
            {
                return Err(io::Error::last_os_error());
            }
            let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFL) };
            if flags < 0
                || unsafe { libc::fcntl(original.fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                    < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        self.enabled = true;
        Ok(())
    }
    pub(super) fn fd(&self) -> Option<RawFd> {
        self.ends[0]
            .as_ref()
            .filter(|original| original.usable())
            .map(Original::fd)
    }
    pub(super) fn signal(&self) {
        if !self.enabled {
            return;
        }
        let Some(fd) = self.ends[1]
            .as_ref()
            .filter(|original| original.usable())
            .map(Original::fd)
        else {
            return;
        };
        // Full pipe already has a pending signal. An uncertain/closed endpoint
        // is never retried after its actual close actor has consumed it.
        let byte = [1_u8];
        loop {
            let count = unsafe { libc::write(fd, byte.as_ptr().cast(), 1) };
            if count >= 0 {
                return;
            }
            if io::Error::last_os_error().kind() != io::ErrorKind::Interrupted {
                return;
            }
        }
    }
    pub(super) fn drain(&self) {
        let Some(fd) = self.fd() else {
            return;
        };
        let mut bytes = [0; 64];
        while unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) } > 0 {}
    }
    pub(super) fn close(&mut self) -> [Option<io::Error>; 2] {
        std::array::from_fn(|index| {
            self.ends[index]
                .as_mut()
                .and_then(|original| original.close_once().err())
        })
    }
    pub(super) fn closed(&self) -> bool {
        self.ends.iter().flatten().all(Original::closed)
    }
}
impl Session {
    pub(super) fn prepare_actor_wake(&self) -> io::Result<()> {
        if self.dedicated_worker {
            let mut wake = self
                .wake_io
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            wake.prepare(0)?;
        }
        Ok(())
    }
    pub(super) fn notify(&self) {
        self.wake_io
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .signal();
        self.wake.notify_all();
    }
    pub(super) fn close_actor_wake(&self) {
        let sources = self
            .wake_io
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
        for (index, source) in sources.into_iter().enumerate() {
            if let Some(source) = source {
                self.failures.retain(
                    0,
                    if index == 0 {
                        Phase::WakeRead
                    } else {
                        Phase::WakeWrite
                    },
                    source,
                );
            }
        }
    }
}
