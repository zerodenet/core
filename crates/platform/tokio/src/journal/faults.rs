//! Typed actual resource sources, independent of physical native closure.
use super::*;
use crate::contract::{DeliveryState, Impact, Reason, Stage};
pub(super) type Sources = [Option<Arc<io::Error>>; 6];
/// Same finite original journal graph's real storage/close errors. The first
/// actual source and every later independent close source remain strongly held
/// by its existing Host receiver. This is never a task or native-close receipt.
#[derive(Debug)]
pub struct JournalFaults {
    first: Arc<io::Error>,
    sources: Arc<Mutex<Sources>>,
}
impl JournalFaults {
    pub fn first(&self) -> &Arc<io::Error> {
        &self.first
    }
    /// work, lock-close, read-close, append-close, temporary-close, path cleanup.
    pub fn sources(&self) -> [Option<Arc<io::Error>>; 6] {
        self.sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}
impl std::fmt::Display for JournalFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.first.fmt(f)
    }
}
impl std::error::Error for JournalFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.first.as_ref())
    }
}
#[derive(Debug)]
struct OriginalIo(Arc<io::Error>);
impl std::fmt::Display for OriginalIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for OriginalIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub(super) fn retained(source: Arc<io::Error>) -> io::Error {
    io::Error::new(source.kind(), OriginalIo(source))
}
pub(super) fn actual<T>(
    result: io::Result<T>,
    source: &mut Option<Arc<io::Error>>,
) -> io::Result<T> {
    result.map_err(|error| retained(source.get_or_insert_with(|| Arc::new(error)).clone()))
}
impl Core {
    pub(super) fn capture_faults(&self, state: &State) {
        let observed = [
            state.work_failure.clone().or_else(|| {
                state
                    .start_failure
                    .as_ref()
                    .and_then(|failure| failure.work.clone())
            }),
            state.lock.close_failure(),
            state.read.close_failure(),
            state.append.close_failure(),
            state.temporary.close_failure(),
            state.path_failure.clone(),
        ];
        let mut sources = self
            .fault_sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (retained, actual) in sources.iter_mut().zip(observed) {
            if retained.is_none() {
                *retained = actual;
            }
        }
    }
    pub(super) fn failure(&self) -> Option<Arc<ChannelError>> {
        match self.state.try_lock() {
            Ok(state) => self.capture_faults(&state),
            Err(std::sync::TryLockError::Poisoned(error)) => {
                self.capture_faults(&error.into_inner())
            }
            Err(std::sync::TryLockError::WouldBlock) => {}
        }
        let first = self
            .fault_sources
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .flatten()
            .next()
            .cloned()?;
        let mut fault = self
            .native_fault
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Some(
            fault
                .get_or_insert_with(|| {
                    let mut error = ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(JournalFaults {
                            first,
                            sources: self.fault_sources.clone(),
                        });
                    error.component = self.hosted.as_ref().map(|hosted| hosted.instance.component);
                    error.delivery = DeliveryState::Unknown;
                    Arc::new(error)
                })
                .clone(),
        )
    }
}
impl JournalOwner {
    pub fn failure(&self) -> Option<Arc<ChannelError>> {
        self.0.failure()
    }
}
