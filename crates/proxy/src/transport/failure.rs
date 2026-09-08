//! Error provenance retained across neutral transport and runtime boundaries.

use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TransportFailureOrigin {
    Client,
    LocalNetwork,
    NameResolution,
    Upstream,
}

#[derive(Debug)]
struct TransportFailure {
    origin: TransportFailureOrigin,
    operation: &'static str,
    source: io::Error,
}

impl fmt::Display for TransportFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.operation, self.source)
    }
}

impl Error for TransportFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.source)
    }
}

pub(crate) fn attributed_error(
    origin: TransportFailureOrigin,
    operation: &'static str,
    source: io::Error,
) -> io::Error {
    io::Error::new(
        source.kind(),
        TransportFailure {
            origin,
            operation,
            source,
        },
    )
}

/// `io::Error::source` can skip its immediate boxed error. Inspect `get_ref`
/// too, so nested runtime/io wrappers do not discard the recorded origin.
pub(crate) fn failure_origin(error: &(dyn Error + 'static)) -> Option<TransportFailureOrigin> {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(failure) = error.downcast_ref::<TransportFailure>() {
            return Some(failure.origin);
        }
        if let Some(zero_engine::EngineError::Io(inner)) =
            error.downcast_ref::<zero_engine::EngineError>()
        {
            current = Some(inner);
            continue;
        }
        if let Some(zero_transport::RuntimeError::Io(inner)) =
            error.downcast_ref::<zero_transport::RuntimeError>()
        {
            current = Some(inner);
            continue;
        }
        if let Some(error) = error.downcast_ref::<io::Error>() {
            if let Some(failure) = error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<TransportFailure>())
            {
                return Some(failure.origin);
            }
            if is_local_network_error(error) {
                return Some(TransportFailureOrigin::LocalNetwork);
            }
            if let Some(inner) = error.get_ref() {
                current = Some(inner);
                continue;
            }
        }
        current = error.source();
    }
    None
}

pub(crate) fn is_local_network_error(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::NetworkDown
            | io::ErrorKind::NetworkUnreachable
            | io::ErrorKind::HostUnreachable
    )
}

#[cfg(test)]
mod tests;
