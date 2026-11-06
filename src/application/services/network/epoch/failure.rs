//! The returned cleanup fault retains this exact retired Epoch and every source.
use super::*;
use std::{error::Error, fmt};
enum Head {
    Exit(Arc<TaskExit>),
    Report(Arc<CloseReport>),
    Io(Arc<io::Error>),
}
struct RetirementFailure {
    // This is the actual finite receiver of all original typed faults. It does
    // not stand for native closure; confirmed was set only by real cleanup.
    _owner: Arc<Epoch>,
    head: Head,
}
impl fmt::Debug for RetirementFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetirementFailure").finish_non_exhaustive()
    }
}
impl fmt::Display for RetirementFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("event generation retired with retained original failures")
    }
}
impl Error for RetirementFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.head {
            Head::Exit(exit) => exit.result.as_ref().err().map(|source| source as _),
            Head::Report(report) => report
                .failed_tasks
                .first()
                .map(|failure| &failure.error as _),
            Head::Io(source) => Some(source.as_ref()),
        }
    }
}
impl Epoch {
    pub(super) fn retained_failure(self: &Arc<Self>) -> Option<io::Error> {
        let head = self
            .failures
            .lock()
            .unwrap()
            .first()
            .cloned()
            .map(Head::Exit)
            .or_else(|| {
                self.task_reports
                    .lock()
                    .unwrap()
                    .first()
                    .cloned()
                    .map(Head::Report)
            })
            .or_else(|| self.faults.lock().unwrap().first().cloned().map(Head::Io))?;
        Some(io::Error::other(RetirementFailure {
            _owner: self.clone(),
            head,
        }))
    }
}

#[derive(Debug)]
struct AttemptFailure {
    operation: Option<Arc<io::Error>>,
    close: Arc<io::Error>,
}
impl fmt::Display for AttemptFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("original event connect and native cleanup failures retained")
    }
}
impl Error for AttemptFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.operation
            .as_ref()
            .map(|source| source.as_ref() as _)
            .or(Some(self.close.as_ref() as _))
    }
}
pub(super) fn attempt_failure(original: &Original, close: Arc<io::Error>) -> Arc<io::Error> {
    Arc::new(io::Error::other(AttemptFailure {
        operation: original.operation_fault.lock().unwrap().clone(),
        close,
    }))
}
