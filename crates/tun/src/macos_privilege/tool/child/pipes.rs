//! Drain both outputs while feeding stdin; one fixed tool, no pipe reader task.
use super::*;
impl ChildOriginal {
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
