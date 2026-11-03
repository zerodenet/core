use std::{fmt, io};

/// Actual deployment-source custody outcome. A write count is never evidence
/// that a failed atomic replacement left the original file intact.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SourceDisposition {
    #[default]
    Unchanged,
    Persisted,
    Restored,
    Committed,
    Unknown,
}
#[derive(Debug, Clone, Default)]
pub struct SourceWriteObservation {
    pub successful_writes: u64,
    pub writable: Option<bool>,
    pub observed_at_unix_ms: Option<u64>,
    pub reason: Option<String>,
    pub disposition: SourceDisposition,
}
#[derive(Debug)]
pub struct SourcePersistError {
    pub disposition: SourceDisposition,
    source: io::Error,
}
impl SourcePersistError {
    pub(super) fn new(disposition: SourceDisposition, source: io::Error) -> Self {
        Self {
            disposition,
            source,
        }
    }
    pub fn kind(&self) -> io::ErrorKind {
        self.source.kind()
    }
}
impl fmt::Display for SourcePersistError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} (source disposition: {:?})",
            self.source, self.disposition
        )
    }
}
impl std::error::Error for SourcePersistError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
impl From<SourcePersistError> for io::Error {
    fn from(error: SourcePersistError) -> Self {
        io::Error::new(error.kind(), error)
    }
}
