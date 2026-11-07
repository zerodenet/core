//! Same actual quota child receives its one original joined worker error.
//! These diagnostics confer no std-thread join, native close or IO authority.
use super::*;
#[derive(Default)]
pub(super) struct WorkerReceiver(Mutex<Option<WorkerFailure>>);
struct WorkerFailure {
    original: Arc<io::Error>,
    reported: bool,
}
#[derive(Debug)]
struct ForeignFailure {
    original: Arc<io::Error>,
    attempted: Arc<io::Error>,
}
impl std::fmt::Display for ForeignFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "quota worker fault differs from its retained original: {}; {}",
            self.original, self.attempted
        )
    }
}
impl std::error::Error for ForeignFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.original.as_ref())
    }
}
impl QuotaStorage {
    /// Actual persistence caller hands off only after consuming its original
    /// std JoinHandle result. `reported` denotes synchronous error delivery,
    /// never a native receipt; later fallback cannot undo a prior delivery.
    pub fn receive_worker_failure(
        &self,
        original: Arc<io::Error>,
        reported: bool,
    ) -> io::Result<()> {
        let mut receiver = self
            .worker
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match receiver.as_mut() {
            Some(current) if Arc::ptr_eq(&current.original, &original) => {
                current.reported |= reported;
            }
            Some(current) => {
                return Err(io::Error::other(ForeignFailure {
                    original: current.original.clone(),
                    attempted: original,
                }))
            }
            None => *receiver = Some(WorkerFailure { original, reported }),
        }
        Ok(())
    }
    /// Read-only exact source, not evidence of worker/native retirement.
    pub fn retained_worker_failure(&self) -> Option<Arc<io::Error>> {
        self.worker
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_ref()
            .map(|failure| failure.original.clone())
    }
    pub(super) fn report_worker_failure(&self) -> Option<io::Error> {
        let mut receiver = self
            .worker
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let failure = receiver.as_mut()?;
        if failure.reported {
            return None;
        }
        failure.reported = true;
        Some(retained(&failure.original))
    }
}
