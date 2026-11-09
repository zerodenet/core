//! The one helper-side close actor; partial IPC keeps its original offsets.
use super::*;
pub(in crate::macos_privilege) struct Original {
    fd: RawFd,
    id: u64,
    close: Option<Result<(), i32>>,
    ack: [u8; 8],
    read: usize,
    accepted: bool,
    reply: [u8; 12],
    written: usize,
}
impl Original {
    pub(in crate::macos_privilege) fn new(fd: RawFd, id: u64) -> Self {
        Self {
            fd,
            id,
            close: None,
            ack: [0; 8],
            read: 0,
            accepted: false,
            reply: [0; 12],
            written: 0,
        }
    }
    pub(in crate::macos_privilege) fn fd(&self) -> RawFd {
        self.fd
    }
    pub(in crate::macos_privilege) fn closed(&self) -> bool {
        self.close == Some(Ok(()))
    }
    pub(in crate::macos_privilege) fn usable(&self) -> bool {
        self.close.is_none()
    }
    pub(in crate::macos_privilege) fn close_once(&mut self) -> io::Result<()> {
        let outcome = *self.close.get_or_insert_with(|| {
            if unsafe { libc::close(self.fd) } == 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error()
                    .raw_os_error()
                    .unwrap_or(libc::EIO))
            }
        });
        outcome.map_err(io::Error::from_raw_os_error)
    }
    pub(in crate::macos_privilege) fn finish(&mut self, stream: &mut UnixStream) -> io::Result<()> {
        use std::io::{Read, Write};
        // WouldBlock/timeout preserves every offset. No retry can reread the
        // parent's nonce, repeat close(2), or start the reply again at byte 0.
        while self.read < self.ack.len() {
            match stream.read(&mut self.ack[self.read..]) {
                Ok(0) => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
                Ok(count) => self.read += count,
                Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                Err(source) => return Err(source),
            }
        }
        if !self.accepted {
            if u64::from_be_bytes(self.ack) != self.id {
                return Err(io::Error::other(
                    "parent did not retain the matching helper original",
                ));
            }
            self.accepted = true;
            let close = self.close_once();
            self.reply[..8].copy_from_slice(&self.id.to_be_bytes());
            self.reply[8..].copy_from_slice(
                &close
                    .as_ref()
                    .err()
                    .map_or(0, |source| source.raw_os_error().unwrap_or(libc::EIO))
                    .to_be_bytes(),
            );
        }
        while self.written < self.reply.len() {
            match stream.write(&self.reply[self.written..]) {
                Ok(0) => return Err(io::Error::from_raw_os_error(libc::EPIPE)),
                Ok(count) => self.written += count,
                Err(source) if source.kind() == io::ErrorKind::Interrupted => continue,
                Err(source) => return Err(source),
            }
        }
        self.close.unwrap().map_err(io::Error::from_raw_os_error)
    }
}
