//! Only the actual output shapes of the listener's existing child tasks.
use super::Source;
use std::sync::Arc;
use zero_engine::EngineError;
pub(crate) trait TaskOutput: Send + 'static {
    fn into_failure(self) -> Option<Source>;
}
impl TaskOutput for () {
    fn into_failure(self) -> Option<Source> {
        None
    }
}
impl TaskOutput for Result<(), EngineError> {
    fn into_failure(self) -> Option<Source> {
        self.err().map(|error| Arc::new(error) as Source)
    }
}
impl TaskOutput for Result<(), std::io::Error> {
    fn into_failure(self) -> Option<Source> {
        self.err().map(|error| Arc::new(error) as Source)
    }
}
#[derive(Debug)]
struct AssociationFailure {
    source: zero_traits::SocketAddress,
    association: u64,
    error: EngineError,
}
impl std::fmt::Display for AssociationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "UDP association {} at {:?}: {}",
            self.association, self.source, self.error
        )
    }
}
impl std::error::Error for AssociationFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
impl TaskOutput for (zero_traits::SocketAddress, u64, Result<(), EngineError>) {
    fn into_failure(self) -> Option<Source> {
        self.2.err().map(|error| {
            Arc::new(AssociationFailure {
                source: self.0,
                association: self.1,
                error,
            }) as Source
        })
    }
}
