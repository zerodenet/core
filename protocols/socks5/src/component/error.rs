use std::{error::Error, fmt, io};

/// Preserve the established wire error text and the original carrier source.
#[derive(Debug)]
pub(super) struct HandshakeError {
    pub(super) wire: zero_core::Error,
    pub(super) source: Option<io::Error>,
}
impl fmt::Display for HandshakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.wire.fmt(f)
    }
}
impl Error for HandshakeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_ref()
            .map(|source| source as &(dyn Error + 'static))
    }
}
