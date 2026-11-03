//! Preserve the original diagnostic and typed channel source at the legacy
//! Engine error boundary. ChannelError's own Display intentionally has no cause.
use std::{error::Error, fmt, io};
use zero_platform_tokio::contract::ChannelError;
#[derive(Debug)]
struct Failure(ChannelError);
impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)?;
        if let Some(source) = self.0.source() {
            write!(f, ": {source}")?;
        }
        Ok(())
    }
}
impl Error for Failure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}
pub(super) fn into_io(error: ChannelError) -> io::Error {
    io::Error::other(Failure(error))
}
