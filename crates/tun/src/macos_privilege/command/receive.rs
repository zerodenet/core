//! Same RPC continuation, including paid bytes after the original caller stops.
use super::{
    wire::{self, Finished, Frame, Kind},
    CommandBytes, CommandOutput,
};
use crate::packet_memory::{PacketMemoryError, PacketMemoryView};
use std::{
    io,
    os::{fd::RawFd, unix::process::ExitStatusExt},
    process::ExitStatus,
};
pub(in crate::macos_privilege) struct Receiving {
    nonce: u64,
    header: [u8; wire::HEADER],
    read: usize,
    frame: Option<Frame>,
    consumed: usize,
    trailer: [u8; wire::FINISH],
    finished: Option<Finished>,
    output_error: Option<PacketMemoryError>,
    stdout: CommandBytes,
    stderr: CommandBytes,
}
impl Receiving {
    pub(in crate::macos_privilege) fn new(
        nonce: u64,
        storage: &[crate::packet_memory::PacketMetadataArc<super::inline::Storage>; 2],
    ) -> io::Result<Self> {
        let stdout = super::inline::Storage::claim(&storage[0])?;
        let stderr = super::inline::Storage::claim(&storage[1])?;
        Ok(Self {
            nonce,
            header: [0; wire::HEADER],
            read: 0,
            frame: None,
            consumed: 0,
            trailer: [0; wire::FINISH],
            finished: None,
            output_error: None,
            stdout: CommandBytes::inline(stdout),
            stderr: CommandBytes::inline(stderr),
        })
    }
    pub(in crate::macos_privilege) fn progress(
        &mut self,
        fd: RawFd,
        scratch: &mut [u8],
        memory: &PacketMemoryView,
        limit: usize,
    ) -> io::Result<bool> {
        if scratch.is_empty() {
            return Err(io::Error::other(
                "same command lacks its original fixed receive scratch",
            ));
        }
        while self.finished.is_none() {
            if self.frame.is_none() {
                while self.read < wire::HEADER {
                    let Some(count) = read(fd, &mut self.header[self.read..])? else {
                        return Ok(false);
                    };
                    self.read += count;
                }
                self.frame = Some(Frame::decode(&self.header, self.nonce)?);
            }
            let frame = self.frame.as_ref().unwrap();
            let remaining = frame.length - self.consumed;
            let size = match frame.kind {
                Kind::Finish => remaining.min(scratch.len()),
                kind => {
                    let bytes = if kind == Kind::Stdout {
                        &mut self.stdout
                    } else {
                        &mut self.stderr
                    };
                    if self.output_error.is_none() {
                        if let Err(source) = bytes.reserve_owned(memory, limit) {
                            self.output_error = Some(source);
                        }
                    }
                    // Capacity is reserved BEFORE the next actual socket read.
                    // An explicit storage fault drains on the already paid
                    // scratch; it cannot manufacture a successful output.
                    let room = if self.output_error.is_some() {
                        scratch.len()
                    } else {
                        (bytes.capacity() - bytes.len()).min(limit.saturating_sub(bytes.len()))
                    };
                    remaining.min(room).min(scratch.len())
                }
            };
            let Some(count) = read(fd, &mut scratch[..size])? else {
                return Ok(false);
            };
            match frame.kind {
                Kind::Finish => self.trailer[self.consumed..self.consumed + count]
                    .copy_from_slice(&scratch[..count]),
                kind if self.output_error.is_none() => {
                    let bytes = if kind == Kind::Stdout {
                        &mut self.stdout
                    } else {
                        &mut self.stderr
                    };
                    bytes.append_reserved(&scratch[..count])?;
                }
                _ => {}
            }
            self.consumed += count;
            if self.consumed == frame.length {
                if frame.kind == Kind::Finish {
                    self.finished = Some(Finished::decode(&self.trailer));
                }
                self.frame = None;
                self.read = 0;
                self.consumed = 0;
            }
        }
        Ok(true)
    }
    pub(in crate::macos_privilege) fn finished(&self) -> Option<&Finished> {
        self.finished.as_ref()
    }
    /// Caller must first consume its independent actual tool-exit observation.
    pub(in crate::macos_privilege) fn take(self) -> io::Result<CommandOutput> {
        let done = self
            .finished
            .ok_or_else(|| io::Error::from(io::ErrorKind::WouldBlock))?;
        if self.output_error.is_some()
            || done.operation_errno != 0
            || done.wait_errno != 0
            || done.close_errnos.iter().any(|errno| *errno != 0)
        {
            return Err(io::Error::other(RemoteFailure {
                storage: self.output_error,
                remote: done,
            }));
        }
        Ok(CommandOutput {
            status: ExitStatus::from_raw(done.status),
            stdout: self.stdout,
            stderr: self.stderr,
        })
    }
}
fn read(fd: RawFd, bytes: &mut [u8]) -> io::Result<Option<usize>> {
    loop {
        let count = unsafe { libc::read(fd, bytes.as_mut_ptr().cast(), bytes.len()) };
        if count > 0 {
            return Ok(Some(count as usize));
        }
        if count == 0 {
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
        let source = io::Error::last_os_error();
        match source.kind() {
            io::ErrorKind::Interrupted => continue,
            io::ErrorKind::WouldBlock => return Ok(None),
            _ => return Err(source),
        }
    }
}
struct RemoteFailure {
    storage: Option<PacketMemoryError>,
    remote: Finished,
}
impl std::fmt::Debug for RemoteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
impl std::fmt::Display for RemoteFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,"actual fixed command faults: operation errno {}, wait errno {}, original close errnos {:?}",self.remote.operation_errno,self.remote.wait_errno,self.remote.close_errnos)
    }
}
impl std::error::Error for RemoteFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.storage.as_ref().map(|source| source as _)
    }
}

#[cfg(test)]
#[path = "receive/tests.rs"]
mod tests;
