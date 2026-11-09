//! Offset continuation is distinct from pure outcome observation.
use super::*;
use std::io::{Read, Write};
#[derive(Default)]
pub(super) struct Progress {
    written: usize,
    reply: [u8; 12],
    read: usize,
    complete: bool,
}
impl Transfer {
    pub(in crate::macos_privilege) fn confirm(
        &self,
        stream: &mut UnixStream,
        fd: RawFd,
    ) -> io::Result<()> {
        if self.received.get() != Some(&fd) {
            return Err(io::Error::other(
                "actual parent owner has not retained this descriptor",
            ));
        }
        let mut progress = self.progress.lock().unwrap_or_else(|e| e.into_inner());
        if !progress.complete {
            let bytes = self.id.to_be_bytes();
            while progress.written < bytes.len() {
                match stream.write(&bytes[progress.written..]) {
                    Ok(0) => return Err(io::Error::from_raw_os_error(libc::EPIPE)),
                    Ok(size) => progress.written += size,
                    Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                    Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                        wait_io(stream, libc::POLLOUT)?
                    }
                    Err(source) => return Err(source),
                }
            }
            while progress.read < progress.reply.len() {
                let offset = progress.read;
                match stream.read(&mut progress.reply[offset..]) {
                    Ok(0) => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "helper close acknowledgement is incomplete",
                        ))
                    }
                    Ok(size) => progress.read += size,
                    Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                    Err(source) if source.kind() == io::ErrorKind::WouldBlock => {
                        wait_io(stream, libc::POLLIN)?
                    }
                    Err(source) => return Err(source),
                }
            }
            if u64::from_be_bytes(progress.reply[..8].try_into().unwrap()) != self.id {
                return Err(io::Error::other(
                    "helper close receipt belongs to another transfer",
                ));
            }
            let errno = i32::from_be_bytes(progress.reply[8..].try_into().unwrap());
            self.observe_remote_close(errno);
            progress.complete = true;
        }
        drop(progress);
        self.require_closed()
    }
}
fn wait_io(stream: &UnixStream, events: i16) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let mut fd = libc::pollfd {
        fd: stream.as_raw_fd(),
        events,
        revents: 0,
    };
    loop {
        let result = unsafe { libc::poll(&mut fd, 1, -1) };
        if result >= 0 {
            return Ok(());
        }
        let source = io::Error::last_os_error();
        if source.kind() != io::ErrorKind::Interrupted {
            return Err(source);
        }
    }
}
