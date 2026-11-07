//! Actual acquisition and physical retirement; no Scope self-wait.
use super::*;
use io_ops::regular_options;
impl Core {
    pub(super) fn start(self: &Arc<Self>) -> io::Result<()> {
        self.require_scope()?;
        self.reserve()?;
        let mut state = self.call_state()?;
        if state.started {
            return self.require_open(&state);
        }
        if let Some(failure) = &state.start_failure {
            return Err(io::Error::new(io::ErrorKind::Other, failure.clone()));
        }
        if state.closing {
            return Err(io::Error::new(io::ErrorKind::BrokenPipe, "journal retired"));
        }
        let work = (|| {
            if let Some(parent) = self
                .spec
                .path
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
                #[cfg(unix)]
                if self.spec.parent_private {
                    use std::os::unix::fs::PermissionsExt;
                    std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700))?;
                }
            }
            let mut options = regular_options(&self.spec.path.with_extension("lock"))?;
            let file = options
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(self.spec.path.with_extension("lock"))?;
            state.lock.install(file);
            state.lock.original().validate_regular()?;
            state
                .lock
                .original()
                .try_lock()
                .map_err(|error| io::Error::new(io::ErrorKind::AlreadyExists, error))?;
            Ok(())
        })();
        if let Err(work) = work {
            let error = JournalFailure {
                work: Some(Arc::new(work)),
                cleanup: state.lock.close().err(),
            };
            state.start_failure = Some(error.clone());
            return Err(io::Error::new(error.work().unwrap().kind(), error));
        }
        state.started = true;
        Ok(())
    }
    pub(super) fn try_close(&self) -> io::Result<Option<JournalClosed>> {
        self.closing.store(true, Ordering::Release);
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return Ok(None),
        };
        let result = state.close_all();
        self.capture_faults(&state);
        result.map(Some)
    }
}
