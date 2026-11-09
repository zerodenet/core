//! Drain both outputs while feeding stdin; one fixed tool, no pipe reader task.
use super::*;
impl ChildOriginal {
    pub(super) fn prepare_pipes(&mut self, input: bool) -> io::Result<()> {
        for index in 0..3 {
            if index == 0 && !input {
                let fd =
                    unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY | libc::O_CLOEXEC) };
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                self.preparing[0] = Some(Original::new(fd, 0));
                if fd <= 2 {
                    let relocated = unsafe { libc::fcntl(fd, libc::F_DUPFD_CLOEXEC, 3) };
                    if relocated < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    self.relocated[0] = Some(Original::new(relocated, 0));
                    self.preparing[0].as_mut().unwrap().close_once()?;
                }
                self.child_pipes[0] = self.relocated[0]
                    .take()
                    .or_else(|| self.preparing[0].take());
                continue;
            }
            let mut pair = [-1; 2];
            if unsafe { libc::pipe(pair.as_mut_ptr()) } < 0 {
                return Err(io::Error::last_os_error());
            }
            self.preparing = [
                Some(Original::new(pair[0], 0)),
                Some(Original::new(pair[1], 0)),
            ];
            for original in self.preparing.iter().flatten() {
                let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFD) };
                if flags < 0
                    || unsafe {
                        libc::fcntl(original.fd(), libc::F_SETFD, flags | libc::FD_CLOEXEC)
                    } < 0
                {
                    return Err(io::Error::last_os_error());
                }
            }
            // Keep child sources above standard descriptors so dup2 cannot
            // overwrite a different pipe when the application started without
            // one of stdin/stdout/stderr. Retain both originals before close.
            for slot in 0..2 {
                let original = self.preparing[slot].as_mut().unwrap();
                if original.fd() <= 2 {
                    let fd = unsafe { libc::fcntl(original.fd(), libc::F_DUPFD_CLOEXEC, 3) };
                    if fd < 0 {
                        return Err(io::Error::last_os_error());
                    }
                    self.relocated[slot] = Some(Original::new(fd, 0));
                    original.close_once()?; // both originals are durable in self
                }
            }
            let mut ends = [None, None];
            for slot in 0..2 {
                ends[slot] = self.relocated[slot]
                    .take()
                    .or_else(|| self.preparing[slot].take());
            }
            let [read, write] = ends;
            if index == 0 {
                self.pipes[index] = write;
                self.child_pipes[index] = read;
            } else {
                self.pipes[index] = read;
                self.child_pipes[index] = write;
            }
        }
        Ok(())
    }
    pub(super) fn close_child_ends(&mut self) -> io::Result<()> {
        let mut failed = None;
        for original in self
            .child_pipes
            .iter_mut()
            .chain(&mut self.preparing)
            .chain(&mut self.relocated)
            .flatten()
        {
            if let Err(source) = original.close_once() {
                failed.get_or_insert(source);
            }
        }
        failed.map_or(Ok(()), Err)
    }

    pub(super) fn collect(&mut self, input: &[u8], limit: usize) -> io::Result<(Vec<u8>, Vec<u8>)> {
        for original in self.pipes.iter().flatten() {
            let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFL) };
            if flags < 0
                || unsafe { libc::fcntl(original.fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                    < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let mut output = [Vec::new(), Vec::new()];
        let mut offset = 0;
        let mut full = false;
        while self.pipes.iter().flatten().any(Original::usable) {
            let mut poll = [libc::pollfd {
                fd: -1,
                events: 0,
                revents: 0,
            }; 3];
            for (index, original) in self.pipes.iter().enumerate() {
                if let Some(original) = original.as_ref().filter(|original| original.usable()) {
                    poll[index].fd = original.fd();
                    poll[index].events = if index == 0 {
                        libc::POLLOUT
                    } else {
                        libc::POLLIN
                    };
                }
            }
            if offset == input.len() {
                if let Some(stdin) = self.pipes[0].as_mut().filter(|original| original.usable()) {
                    stdin.close_once()?;
                }
                poll[0].fd = -1;
            }
            if poll.iter().all(|fd| fd.fd == -1) {
                break;
            }
            if unsafe { libc::poll(poll.as_mut_ptr(), poll.len() as _, -1) } < 0 {
                let source = io::Error::last_os_error();
                if source.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(source);
            }
            for (index, polled) in poll.iter().enumerate() {
                if polled.fd < 0 || polled.revents == 0 {
                    continue;
                }
                if index == 0 {
                    let count = unsafe {
                        libc::write(
                            polled.fd,
                            input[offset..].as_ptr().cast(),
                            input.len() - offset,
                        )
                    };
                    if count > 0 {
                        offset += count as usize;
                    } else if count < 0 {
                        check_error()?;
                    }
                } else {
                    let mut bytes = [0_u8; 4096];
                    let count =
                        unsafe { libc::read(polled.fd, bytes.as_mut_ptr().cast(), bytes.len()) };
                    if count == 0 {
                        self.pipes[index].as_mut().unwrap().close_once()?;
                    } else if count > 0 {
                        let destination = &mut output[index - 1];
                        let room = limit.saturating_sub(destination.len());
                        let keep = room.min(count as usize);
                        destination.extend_from_slice(&bytes[..keep]);
                        full |= keep != count as usize;
                    } else {
                        check_error()?;
                    }
                }
            }
        }
        if full {
            return Err(io::Error::other(
                "fixed tool output exceeds candidate file-read allowance",
            ));
        }
        let [stdout, stderr] = output;
        Ok((stdout, stderr))
    }
}
fn check_error() -> io::Result<()> {
    let source = io::Error::last_os_error();
    if matches!(
        source.kind(),
        io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
    ) {
        Ok(())
    } else {
        Err(source)
    }
}
