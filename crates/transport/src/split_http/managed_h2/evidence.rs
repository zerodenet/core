use std::{
    error::Error,
    io,
    sync::{Arc, OnceLock},
};

/// The actual poll operation which returned the retained original I/O error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IoOperation {
    Read,
    Write,
    WriteVectored,
    Flush,
    Shutdown,
}

#[derive(Debug)]
struct ObservedIo {
    operation: IoOperation,
    error: Arc<io::Error>,
}
#[derive(Debug, Default)]
struct Sources {
    io: OnceLock<ObservedIo>,
    h2: OnceLock<Arc<h2::Error>>,
    hyper: OnceLock<Arc<hyper::Error>>,
}
/// Bounded evidence: at most the first actual error of each type, no history.
/// Later observations append diagnostic evidence; they never alter the first
/// ChannelError Arc or any of its decision fields. Missing types stay absent.
#[derive(Clone, Debug, Default)]
pub struct H2ConnectionFailure(Arc<Sources>);
impl H2ConnectionFailure {
    /// This is the original object, including its original raw_os_error(). The
    /// io::Error facade returned to Hyper deliberately has no raw OS code.
    pub fn io_source(&self) -> Option<&Arc<io::Error>> {
        self.0.io.get().map(|source| &source.error)
    }
    pub fn io_operation(&self) -> Option<IoOperation> {
        self.0.io.get().map(|source| source.operation)
    }
    pub fn h2_source(&self) -> Option<&h2::Error> {
        self.0.h2.get().map(Arc::as_ref).or_else(|| {
            self.hyper_source()
                .and_then(|error| error.source())
                .and_then(|source| source.downcast_ref::<h2::Error>())
        })
    }
    /// The exact owned object captured at ConnMapErr, when that path occurred.
    pub fn h2_source_arc(&self) -> Option<&Arc<h2::Error>> {
        self.0.h2.get()
    }
    pub fn hyper_source(&self) -> Option<&Arc<hyper::Error>> {
        self.0.hyper.get()
    }
    pub(super) fn record_io(&self, operation: IoOperation, error: Arc<io::Error>) {
        let _ = self.0.io.set(ObservedIo { operation, error });
    }
    pub(super) fn record_h2(&self, error: Arc<h2::Error>) {
        let _ = self.0.h2.set(error);
    }
    pub(super) fn record_hyper(&self, error: Arc<hyper::Error>) {
        let _ = self.0.hyper.set(error);
    }
}
impl std::fmt::Display for H2ConnectionFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("original H2 failure evidence")
    }
}
impl Error for H2ConnectionFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if let Some(error) = self.hyper_source() {
            Some(error.as_ref())
        } else if let Some(error) = self.h2_source() {
            Some(error)
        } else {
            self.io_source()
                .map(|error| error.as_ref() as &(dyn Error + 'static))
        }
    }
}
