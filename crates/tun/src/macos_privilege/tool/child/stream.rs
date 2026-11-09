//! Same helper/Child/stdio originals; owned output has no helper heap copy.
use super::*;
use crate::macos_privilege::command::wire;
use std::os::unix::{net::UnixStream, process::ExitStatusExt};
pub(in crate::macos_privilege) fn run_streamed(
    slot: &mut ChildOriginal,
    program: HelperProgram,
    arguments: &[String],
    input: Option<&str>,
    supervisor: (RawFd, u64),
    stream: &mut UnixStream,
    limit: usize,
) -> io::Result<()> {
    let executable = match program {
        HelperProgram::Ifconfig => "/sbin/ifconfig",
        HelperProgram::Route => "/sbin/route",
        HelperProgram::Pfctl => "/sbin/pfctl",
    };
    let started = start_fixed(slot, executable, arguments, input, Some(supervisor));
    let mut operation = None;
    let mut ends = Ok(());
    match started {
        Ok(child_ends) => {
            // Every parent/child end keeps its cached close outcome in slot;
            // collect must still drain the actual Child even if one end failed.
            ends = child_ends;
            if let Err(source) =
                slot.collect_streamed(input.unwrap_or("").as_bytes(), stream, supervisor.1, limit)
            {
                operation = Some(source);
            }
        }
        Err(source) => operation = Some(source),
    }
    let closed = slot.close_pipes();
    let waited = if slot.child.is_some() {
        slot.wait().map(Some)
    } else {
        Ok(None)
    };
    let (status, wait_errno) = match &waited {
        Ok(status) => (status.map_or(0, ExitStatusExt::into_raw), 0),
        Err(source) => (0, errno(source)),
    };
    let close_errnos = std::array::from_fn(|index| {
        let original = match index {
            0..=2 => &slot.pipes[index],
            3..=5 => &slot.child_pipes[index - 3],
            6..=7 => &slot.preparing[index - 6],
            _ => &slot.relocated[index - 8],
        };
        original
            .as_ref()
            .and_then(Original::close_errno)
            .unwrap_or(0)
    });
    // The exact remote syscall outcomes travel as data, not close authority.
    // Parent independently waits for its real registered Child NOTE_EXIT.
    let report = wire::Finished {
        status,
        child_pid: slot.child.as_ref().map_or(0, |child| child.id() as i32),
        operation_errno: operation
            .as_ref()
            .or_else(|| ends.as_ref().err())
            .map_or(0, errno),
        wait_errno,
        close_errnos,
    };
    wire::write(stream, supervisor.1, wire::Kind::Finish, &report.encode())?;
    failures::finish_unit(
        ends,
        operation.map_or(Ok(()), Err),
        closed,
        waited.map(|_| ()),
    )
}
fn errno(source: &io::Error) -> i32 {
    failures::original_errno(source)
}
impl ChildOriginal {
    fn collect_streamed(
        &mut self,
        input: &[u8],
        stream: &mut UnixStream,
        nonce: u64,
        limit: usize,
    ) -> io::Result<()> {
        for original in self.pipes.iter().flatten() {
            let flags = unsafe { libc::fcntl(original.fd(), libc::F_GETFL) };
            if flags < 0
                || unsafe { libc::fcntl(original.fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) }
                    < 0
            {
                return Err(io::Error::last_os_error());
            }
        }
        let mut lengths = [0usize; 2];
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
                        let room = limit.saturating_sub(lengths[index - 1]);
                        let keep = room.min(count as usize);
                        if keep != 0 {
                            wire::write(
                                stream,
                                nonce,
                                if index == 1 {
                                    wire::Kind::Stdout
                                } else {
                                    wire::Kind::Stderr
                                },
                                &bytes[..keep],
                            )?;
                            lengths[index - 1] += keep;
                        }
                        full |= keep != count as usize;
                    } else {
                        check_error()?;
                    }
                }
            }
        }
        if full {
            return Err(io::Error::from_raw_os_error(libc::EFBIG));
        }
        Ok(())
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
