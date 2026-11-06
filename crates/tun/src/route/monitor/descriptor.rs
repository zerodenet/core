//! The original Unix descriptor survives bind and reactor registration errors.
use std::{
    fs::File,
    io,
    os::fd::{AsRawFd, FromRawFd, IntoRawFd, RawFd},
    sync::{Arc, Mutex},
};
#[derive(Debug, Default)]
pub(super) struct Descriptor(Mutex<State>);
#[derive(Debug, Default)]
struct State {
    file: Option<File>,
    attempted: bool,
    closed: Option<Result<(), Arc<io::Error>>>,
    unknown: Option<RawFd>,
}
impl Descriptor {
    pub(super) fn acquire(&self, domain: i32, kind: i32, protocol: i32) -> io::Result<()> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.attempted || state.closed.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "route monitor descriptor was already attempted",
            ));
        }
        state.attempted = true;
        let raw = unsafe { libc::socket(domain, kind, protocol) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // Custody precedes all fallible bind/configure/register operations.
        state.file = Some(unsafe { File::from_raw_fd(raw) });
        Ok(())
    }
    pub(super) fn close(&self) -> io::Result<()> {
        let mut state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        if state.closed.is_none() {
            let outcome = if let Some(file) = state.file.take() {
                let raw = file.into_raw_fd();
                if unsafe { libc::close(raw) } == 0 {
                    Ok(())
                } else {
                    // A failed close can have consumed the descriptor. Keep
                    // its Unknown identity/source and never retry a reused fd.
                    state.unknown = Some(raw);
                    Err(Arc::new(io::Error::last_os_error()))
                }
            } else {
                Ok(()) // This original acquired no descriptor.
            };
            state.closed = Some(outcome);
        }
        state
            .closed
            .as_ref()
            .expect("retained close outcome")
            .as_ref()
            .map(|()| ())
            .map_err(|source| io::Error::new(source.kind(), Shared(source.clone())))
    }
    pub(super) fn is_closed(&self) -> bool {
        let state = self.0.lock().unwrap_or_else(|error| error.into_inner());
        state.unknown.is_none() && matches!(state.closed, Some(Ok(())))
    }
}
impl AsRawFd for Descriptor {
    fn as_raw_fd(&self) -> RawFd {
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .file
            .as_ref()
            .expect("same live registered monitor descriptor")
            .as_raw_fd()
    }
}
/// The reactor borrows the original. Failed registration returns only this
/// view; the retained descriptor owner still performs the matching close.
#[derive(Debug)]
pub(super) struct View(pub(super) Arc<Descriptor>);
impl AsRawFd for View {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}
impl Drop for Descriptor {
    fn drop(&mut self) {
        let _ = self.close();
    }
}
#[derive(Debug)]
struct Shared(Arc<io::Error>);
impl std::fmt::Display for Shared {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Shared {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
