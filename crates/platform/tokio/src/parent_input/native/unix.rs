use super::*;
use std::os::fd::AsRawFd;
#[derive(Default)]
pub(crate) struct Input {
    original: Option<i32>,
    unknown: Option<Arc<io::Error>>,
}
pub(crate) fn duplicate_stdin() -> io::Result<Input> {
    duplicate(std::io::stdin().as_raw_fd())
}
pub(crate) fn duplicate(fd: i32) -> io::Result<Input> {
    let original = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 0) };
    if original < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(Input {
            original: Some(original),
            unknown: None,
        })
    }
}
impl Input {
    pub(crate) fn raw(&self) -> usize {
        self.original.expect("acquired original input") as usize
    }
    pub(crate) fn close(&mut self) -> io::Result<()> {
        if let Some(error) = &self.unknown {
            return Err(retained(error));
        }
        if let Some(original) = self.original {
            if unsafe { libc::close(original) } != 0 {
                let error = Arc::new(io::Error::last_os_error());
                self.unknown = Some(error.clone());
                return Err(retained(&error));
            }
            self.original = None;
        }
        Ok(())
    }
}
pub(crate) fn interrupt(_worker: &std::thread::JoinHandle<io::Result<()>>) -> io::Result<()> {
    Ok(())
}
pub(crate) fn read_to_end(raw: usize, stop: &AtomicBool) -> io::Result<()> {
    let mut buffer = [0u8; 64];
    loop {
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut poll = libc::pollfd {
            fd: raw as i32,
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut poll, 1, 50) };
        if ready < 0 {
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if stop.load(Ordering::Acquire) {
            return Ok(());
        }
        if ready == 0 {
            continue;
        }
        if poll.revents & libc::POLLNVAL != 0 {
            return Err(io::Error::from_raw_os_error(libc::EBADF));
        }
        let count = unsafe { libc::read(raw as i32, buffer.as_mut_ptr().cast(), buffer.len()) };
        if count == 0 {
            return Ok(());
        }
        if count < 0 {
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }
}
