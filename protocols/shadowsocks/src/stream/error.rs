//! io::Error forwards its payload's source, so expose the original protocol error.
use std::{error::Error, fmt, io};

#[derive(Debug)]
struct ProtocolFailure(zero_core::Error);
impl fmt::Display for ProtocolFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
impl Error for ProtocolFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
pub(super) fn protocol_io(error: zero_core::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, ProtocolFailure(error))
}
