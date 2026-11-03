//! The same concrete IPC original remains installed through connect, I/O and close.
use std::{
    fmt, io,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, MutexGuard,
    },
};
use zero_platform_tokio::OriginalLocalStream;
#[derive(Default)]
pub(super) struct IpcOriginal {
    retiring: AtomicBool,
    state: Mutex<State>,
    callbacks: Mutex<usize>,
    callbacks_returned: Condvar,
}
#[derive(Default)]
struct State {
    original: Option<OriginalLocalStream>,
    failure: Option<Arc<io::Error>>,
    #[cfg(test)]
    native_closed: usize,
    #[cfg(test)]
    empty_closed: usize,
}
pub(super) struct Call<T> {
    pub operation: io::Result<T>,
    pub cleanup: io::Result<()>,
    pub admission_stopped: bool,
}
#[derive(Debug)]
struct Retained(Arc<io::Error>);
impl fmt::Display for Retained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn retained(error: &Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), Retained(error.clone()))
}
impl IpcOriginal {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| {
            let mut state = error.into_inner();
            state.failure.get_or_insert_with(|| {
                Arc::new(io::Error::other(
                    "original IPC callback panicked; retirement unconfirmed",
                ))
            });
            state
        })
    }
    pub(super) fn callback(&self) -> Option<Callback<'_>> {
        let mut callbacks = self
            .callbacks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.is_retired() {
            return None;
        }
        *callbacks += 1;
        Some(Callback(self))
    }
    pub(super) fn confirm_original_io_closed(&self) -> io::Result<()> {
        let state = self.lock();
        if let Some(error) = &state.failure {
            return Err(retained(error));
        }
        if state.original.is_some() {
            return Err(io::Error::other("original IPC IO remains in custody"));
        }
        Ok(())
    }
    pub(super) fn is_retired(&self) -> bool {
        self.retiring.load(Ordering::Acquire)
    }
    fn close(state: &mut State) -> io::Result<()> {
        if let Some(original) = state.original.as_mut() {
            match original.close() {
                Ok(receipt) if original.matches_closed(&receipt) => {
                    #[cfg(test)]
                    {
                        if receipt.has_native_original() {
                            state.native_closed += 1;
                        } else {
                            state.empty_closed += 1;
                        }
                    }
                    state.original = None;
                }
                outcome => {
                    let source = outcome.err().unwrap_or_else(|| {
                        io::Error::other("IPC original close receipt mismatched")
                    });
                    let source = Arc::new(source);
                    state.failure.get_or_insert_with(|| source.clone());
                    return Err(retained(&source));
                }
            }
        }
        if let Some(error) = &state.failure {
            return Err(retained(error));
        }
        Ok(())
    }
    pub(super) fn call<T>(
        &self,
        path: &str,
        operation: impl FnOnce(&mut OriginalLocalStream) -> io::Result<T>,
    ) -> Call<T> {
        let mut state = self.lock();
        if self.is_retired() {
            return Call {
                operation: Err(io::Error::other("IPC hook generation retired")),
                cleanup: Self::close(&mut state),
                admission_stopped: true,
            };
        }
        if let Some(error) = &state.failure {
            return Call {
                operation: Err(retained(error)),
                cleanup: Err(retained(error)),
                admission_stopped: false,
            };
        }
        if state.original.is_some() {
            return Call {
                operation: Err(io::Error::other("previous IPC original is still owned")),
                cleanup: Err(io::Error::other(
                    "previous IPC original retirement unconfirmed",
                )),
                admission_stopped: false,
            };
        }
        let original = match OriginalLocalStream::new(path) {
            Ok(original) => original,
            Err(error) => {
                return Call {
                    operation: Err(error),
                    cleanup: Ok(()),
                    admission_stopped: false,
                }
            } // no native acquisition
        };
        state.original = Some(original); // persistent owner before connect/native acquisition
        let result = {
            let original = state.original.as_mut().unwrap();
            original.connect().and_then(|()| operation(original))
        };
        let cleanup = Self::close(&mut state);
        // A confirmed allow/deny or write result is independent from cleanup.
        // In particular native-close failure must never turn a real deny into allow.
        Call {
            operation: result,
            cleanup,
            admission_stopped: false,
        }
    }
    pub(super) fn retire(&self) -> io::Result<()> {
        self.retiring.store(true, Ordering::Release);
        let mut callbacks = self
            .callbacks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        while *callbacks != 0 {
            callbacks = self
                .callbacks_returned
                .wait(callbacks)
                .unwrap_or_else(|error| error.into_inner());
        }
        drop(callbacks);
        // The actual synchronous callbacks have returned, separately from
        // their native-close outcomes and the stop request above.
        Self::close(&mut self.lock())
    }
    #[cfg(test)]
    pub(super) fn observation(&self) -> (usize, usize, bool, bool) {
        let state = self.lock();
        (
            state.native_closed,
            state.empty_closed,
            state.original.is_some(),
            state.failure.is_some(),
        )
    }
}

pub(super) struct Callback<'a>(&'a IpcOriginal);
impl Drop for Callback<'_> {
    fn drop(&mut self) {
        let mut callbacks = self
            .0
            .callbacks
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *callbacks -= 1;
        self.0.callbacks_returned.notify_all();
    }
}
