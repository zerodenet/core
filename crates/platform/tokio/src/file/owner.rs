use super::*;
use std::{
    path::PathBuf,
    sync::{Mutex, MutexGuard, TryLockError},
};

/// A nonconsuming observation; task joining is separately owned by the host.
#[derive(Clone, Debug)]
pub enum FileNativeEvidence {
    KnownNoAcquisition,
    Busy,
    Owned,
    ConfirmedClosed,
    Unknown(Arc<io::Error>),
}
#[derive(Clone, Debug)]
pub struct FileReadEvidence {
    pub native: FileNativeEvidence,
    pub read_source: Option<Arc<io::Error>>,
    pub close_source: Option<Arc<io::Error>>,
}
enum Native {
    None,
    Opening,
    Open(File),
    Closed,
    Unknown(Arc<io::Error>),
}
struct State {
    started: bool,
    native: Native,
    read_source: Option<Arc<io::Error>>,
    close_source: Option<Arc<io::Error>>,
}
/// Install this persistent owner before scheduling the blocking worker. No
/// descriptor clone, task spawn, or implicit Drop result is a close receipt.
pub struct PendingFileRead {
    path: PathBuf,
    max_bytes: usize,
    limit: usize,
    state: Mutex<State>,
}
impl PendingFileRead {
    pub fn new(path: PathBuf, max_bytes: usize) -> io::Result<Self> {
        let limit = max_bytes.checked_add(1).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "file byte bound overflow")
        })?;
        Ok(Self {
            path,
            max_bytes,
            limit,
            state: Mutex::new(State {
                started: false,
                native: Native::None,
                read_source: None,
                close_source: None,
            }),
        })
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn run(&self) -> Result<Vec<u8>, Arc<io::Error>> {
        self.run_with_after_open(|| {})
    }
    /// The callback runs only after the original File is installed. It is useful
    /// for synchronous instrumentation; unwinding leaves the same File owned.
    pub fn run_with_after_open(
        &self,
        after_open: impl FnOnce(),
    ) -> Result<Vec<u8>, Arc<io::Error>> {
        let mut state = self.lock();
        if state.started {
            return Err(Arc::new(io::Error::other("file job already started")));
        }
        state.started = true;
        state.native = Native::Opening;
        match native::open(&self.path) {
            Ok(file) => state.native = Native::Open(file),
            Err(source) => {
                let source = Arc::new(source);
                state.native = Native::None;
                state.read_source = Some(source.clone());
                return Err(source);
            }
        }
        after_open();
        self.read_and_close(&mut state)
    }
    fn read_and_close(&self, state: &mut State) -> Result<Vec<u8>, Arc<io::Error>> {
        let Native::Open(file) = &mut state.native else {
            unreachable!("installed file")
        };
        #[cfg(windows)]
        let result = windows::validate(file)
            .and_then(|()| read_opened_file(file, self.max_bytes, self.limit));
        #[cfg(not(windows))]
        let result = read_opened_file(file, self.max_bytes, self.limit);
        let result = result.map_err(Arc::new);
        if let Err(source) = &result {
            state.read_source = Some(source.clone());
        }
        let close = close_owned(state);
        match result {
            Err(source) => Err(source),
            Ok(bytes) => close.map(|()| bytes),
        }
    }
    /// Call only after consuming the actual worker JoinError/result. try_lock
    /// additionally prevents blocking an async thread behind a running read.
    pub fn close_after_join(&self) -> FileReadEvidence {
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::Poisoned(error)) => error.into_inner(),
            Err(TryLockError::WouldBlock) => return busy(),
        };
        if matches!(state.native, Native::Opening) {
            let source = Arc::new(io::Error::other(
                "file acquisition unwound without a disposition",
            ));
            state.native = Native::Unknown(source.clone());
            state.close_source = Some(source);
        }
        let _ = close_owned(&mut state);
        snapshot(&state)
    }
    pub fn evidence(&self) -> FileReadEvidence {
        match self.state.try_lock() {
            Ok(state) => snapshot(&state),
            Err(TryLockError::Poisoned(error)) => snapshot(&error.into_inner()),
            Err(TryLockError::WouldBlock) => busy(),
        }
    }
    #[cfg(all(test, windows))]
    pub(super) fn adopt_for_test(&self, file: File) {
        self.lock().native = Native::Open(file);
    }
    #[cfg(all(test, windows))]
    pub(super) fn run_adopted_for_test(&self) -> Result<Vec<u8>, Arc<io::Error>> {
        self.read_and_close(&mut self.lock())
    }
}
fn close_owned(state: &mut State) -> Result<(), Arc<io::Error>> {
    if !matches!(state.native, Native::Open(_)) {
        return match &state.native {
            Native::Unknown(source) => Err(source.clone()),
            _ => Ok(()),
        };
    }
    let sentinel = Arc::new(io::Error::other(
        "file native close did not return a disposition",
    ));
    let Native::Open(file) =
        std::mem::replace(&mut state.native, Native::Unknown(sentinel.clone()))
    else {
        unreachable!()
    };
    state.close_source = Some(sentinel);
    match native::close(file) {
        Ok(()) => {
            state.native = Native::Closed;
            state.close_source = None;
            Ok(())
        }
        Err(source) => {
            let source = Arc::new(source);
            state.native = Native::Unknown(source.clone());
            state.close_source = Some(source.clone());
            Err(source)
        }
    }
}
fn busy() -> FileReadEvidence {
    FileReadEvidence {
        native: FileNativeEvidence::Busy,
        read_source: None,
        close_source: None,
    }
}
fn snapshot(state: &State) -> FileReadEvidence {
    FileReadEvidence {
        native: match &state.native {
            Native::None => FileNativeEvidence::KnownNoAcquisition,
            Native::Opening => FileNativeEvidence::Busy,
            Native::Open(_) => FileNativeEvidence::Owned,
            Native::Closed => FileNativeEvidence::ConfirmedClosed,
            Native::Unknown(source) => FileNativeEvidence::Unknown(source.clone()),
        },
        read_source: state.read_source.clone(),
        close_source: state.close_source.clone(),
    }
}

#[cfg(all(test, any(unix, windows)))]
#[path = "../../tests/file/owner.rs"]
mod tests;
