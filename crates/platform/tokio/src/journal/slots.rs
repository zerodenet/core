//! Four reusable concrete positions. Reuse requires this same file's receipt.
use super::*;
#[derive(Default)]
pub(super) struct Slot {
    pub file: Option<OriginalFile>,
    receipt: Option<OriginalFileClosed>,
    failure: Option<Arc<io::Error>>,
}
impl Slot {
    pub fn install(&mut self, file: std::fs::File) {
        assert!(
            self.confirmed(),
            "old journal original must close before reuse"
        );
        self.file = Some(OriginalFile::adopt(file));
        self.receipt = None;
        self.failure = None;
    }
    pub fn close_failure(&self) -> Option<Arc<io::Error>> {
        self.failure.clone()
    }
    pub fn confirmed(&self) -> bool {
        self.file.as_ref().is_none_or(|file| {
            self.receipt
                .as_ref()
                .is_some_and(|receipt| file.matches_closed(receipt))
        })
    }
    pub fn close(&mut self) -> Result<(), Arc<io::Error>> {
        if self.confirmed() {
            return Ok(());
        }
        if let Some(failure) = &self.failure {
            return Err(failure.clone());
        }
        match self
            .file
            .as_mut()
            .expect("unconfirmed original exists")
            .close()
        {
            Ok(receipt) => {
                assert!(self.file.as_ref().unwrap().matches_closed(&receipt));
                self.receipt = Some(receipt);
                Ok(())
            }
            Err(error) => {
                let source = Arc::new(error);
                self.failure = Some(source.clone());
                Err(source)
            }
        }
    }
    pub fn original(&mut self) -> &mut OriginalFile {
        self.file.as_mut().expect("original installed before IO")
    }
}
#[derive(Default)]
pub(super) struct State {
    pub identity: Arc<CoreIdentity>,
    pub closed: Option<JournalClosed>,
    pub closing: bool,
    pub started: bool,
    pub start_failure: Option<JournalFailure>,
    pub append_dirty: bool,
    pub lock: Slot,
    pub read: Slot,
    pub append: Slot,
    pub temporary: Slot,
    pub temporary_path: Option<tempfile::TempPath>,
    pub path_failure: Option<Arc<io::Error>>,
    pub work_failure: Option<Arc<io::Error>>,
}
impl Default for CoreIdentity {
    fn default() -> Self {
        Self
    }
}
impl State {
    pub fn discard_temporary(&mut self) -> io::Result<()> {
        if let Some(path) = &self.temporary_path {
            match std::fs::remove_file(path.as_ref() as &std::path::Path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    let source = Arc::new(error);
                    let source = self.path_failure.get_or_insert(source).clone();
                    return Err(failure(None, Some(source)));
                }
            }
        }
        // TempPath cleanup was disabled at installation; no unobserved retry
        // or implicit unlink can discard this concrete pending operation.
        self.temporary_path.take();
        Ok(())
    }
    pub fn close_all(&mut self) -> io::Result<JournalClosed> {
        self.closing = true;
        // Drive every independent original even if one close remains Unknown.
        let mut first = None;
        for slot in [
            &mut self.read,
            &mut self.append,
            &mut self.temporary,
            &mut self.lock,
        ] {
            if let Err(source) = slot.close() {
                first.get_or_insert(source);
            }
        }
        if let Some(source) = first {
            return Err(failure(None, Some(source)));
        }
        // A TempPath is metadata, not a close receipt. Unlink only after the
        // actual temporary original has matching positive close evidence.
        self.discard_temporary()?;
        let receipt = JournalClosed(self.identity.clone());
        self.closed = Some(receipt.clone());
        Ok(receipt)
    }
}
pub(super) fn failure(work: Option<Arc<io::Error>>, cleanup: Option<Arc<io::Error>>) -> io::Error {
    let kind = work
        .as_ref()
        .or(cleanup.as_ref())
        .map_or(io::ErrorKind::Other, |source| source.kind());
    io::Error::new(kind, JournalFailure { work, cleanup })
}
pub(super) fn finish<T>(work: io::Result<T>, close: Result<(), Arc<io::Error>>) -> io::Result<T> {
    match (work, close) {
        (Ok(value), Ok(())) => Ok(value),
        (work, close) => Err(failure(work.err().map(Arc::new), close.err())),
    }
}

/// Release the original state before notifying the existing Host pump. No
/// wake/callback can reenter while a journal IO mutex remains held.
pub(super) struct Operation<'a> {
    state: Option<std::sync::MutexGuard<'a, State>>,
    changed: &'a tokio::sync::Notify,
    stop: Option<&'a CancellationToken>,
}
impl<'a> Operation<'a> {
    pub fn new(
        state: std::sync::MutexGuard<'a, State>,
        changed: &'a tokio::sync::Notify,
        stop: Option<&'a CancellationToken>,
    ) -> Self {
        Self {
            state: Some(state),
            changed,
            stop,
        }
    }
}
impl std::ops::Deref for Operation<'_> {
    type Target = State;
    fn deref(&self) -> &State {
        self.state.as_deref().unwrap()
    }
}
impl std::ops::DerefMut for Operation<'_> {
    fn deref_mut(&mut self) -> &mut State {
        self.state.as_deref_mut().unwrap()
    }
}
impl Drop for Operation<'_> {
    fn drop(&mut self) {
        let failed = self
            .state
            .as_deref()
            .is_some_and(|state| state.work_failure.is_some() || state.start_failure.is_some());
        drop(self.state.take());
        if failed {
            if let Some(stop) = self.stop {
                stop.cancel();
            }
        }
        self.changed.notify_waiters();
    }
}
