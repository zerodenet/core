//! A single original descriptor and a single cached close attempt.
use super::{faults::Attempt, RouteFileFaults};
use crate::packet_memory::PacketMetadataArc;
use std::{fs::File, io};
#[derive(Debug)]
enum State {
    Pending,
    Open(File),
    Closed { original: usize },
    Unknown { original: usize, code: i32 },
}
pub(in crate::route) struct TrackedFile {
    state: State,
    faults: RouteFileFaults,
    attempt: PacketMetadataArc<Attempt>,
    unlocked: bool,
    locked: bool,
    owner_attempted: bool,
    owner_retired: bool,
    #[cfg(test)]
    close_calls: usize,
}
impl std::fmt::Debug for TrackedFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("RouteFileOriginal")
            .field(&match &self.state {
                State::Closed { original } | State::Unknown { original, .. } => Some(*original),
                _ => None,
            })
            .field(&self.state)
            .finish()
    }
}
impl TrackedFile {
    pub(in crate::route) fn pending(
        faults: RouteFileFaults,
        attempt: PacketMetadataArc<Attempt>,
    ) -> Self {
        Self {
            state: State::Pending,
            faults,
            attempt,
            unlocked: false,
            locked: false,
            owner_attempted: false,
            owner_retired: false,
            #[cfg(test)]
            close_calls: 0,
        }
    }
    pub(in crate::route) fn install(&mut self, file: File) {
        assert!(matches!(self.state, State::Pending));
        self.state = State::Open(file)
    }
    pub(in crate::route) fn file(&self) -> io::Result<&File> {
        match &self.state {
            State::Open(f) => Ok(f),
            _ => Err(io::Error::other("original route File is not usable")),
        }
    }
    pub(in crate::route) fn locked(&mut self) {
        self.locked = true
    }
    pub(in crate::route) fn publishing_owner(&mut self) {
        self.owner_attempted = true
    }
    pub(in crate::route) fn retire_owner(&mut self, path: &std::path::Path) {
        if !self.owner_attempted || self.owner_retired {
            return;
        }
        self.owner_retired = true;
        if let Err(source) = std::fs::remove_file(path) {
            if source.kind() != io::ErrorKind::NotFound {
                self.faults.retain(&self.attempt, 1, source)
            }
        }
    }
    pub(in crate::route) fn closed(&self) -> bool {
        matches!(self.state, State::Pending | State::Closed { .. })
    }
    pub(in crate::route) fn close(&mut self) -> io::Result<()> {
        if let State::Unknown { code, .. } = self.state {
            return Err(io::Error::from_raw_os_error(code));
        }
        if self.closed() {
            return Ok(());
        }
        if self.locked && !self.unlocked {
            self.unlocked = true;
            if let Err(source) = fs2::FileExt::unlock(self.file()?) {
                self.faults.retain(&self.attempt, 2, source);
            }
        }
        let State::Open(file) = std::mem::replace(&mut self.state, State::Pending) else {
            unreachable!()
        };
        #[cfg(test)]
        {
            self.close_calls += 1;
        }
        let (identity, outcome) = close_file(file);
        match outcome {
            Ok(()) => {
                self.state = State::Closed { original: identity };
                Ok(())
            }
            Err(source) => {
                let code = source.raw_os_error().unwrap_or(5);
                self.state = State::Unknown {
                    original: identity,
                    code,
                };
                self.faults.retain(&self.attempt, 3, source);
                Err(io::Error::from_raw_os_error(code))
            }
        }
    }
}
#[cfg(unix)]
fn close_file(file: File) -> (usize, io::Result<()>) {
    use std::os::fd::IntoRawFd;
    let fd = file.into_raw_fd();
    let outcome = if unsafe { libc::close(fd) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    };
    (fd as usize, outcome)
}
#[cfg(windows)]
fn close_file(file: File) -> (usize, io::Result<()>) {
    use std::os::windows::io::IntoRawHandle;
    let handle = file.into_raw_handle();
    let outcome = if unsafe { windows_sys::Win32::Foundation::CloseHandle(handle) } != 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    };
    (handle as usize, outcome)
}

#[cfg(test)]
mod tests;
