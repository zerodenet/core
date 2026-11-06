//! Candidate continuation requires the same endpoint's confirmed shutdown.
use std::io;
pub(super) enum Attempt {
    Settled(io::Result<Vec<u8>>),
    Unsettled(io::Error),
}
impl Attempt {
    pub(super) fn finish(query: io::Result<Vec<u8>>, close: io::Result<()>) -> Self {
        match close {
            Ok(()) => Self::Settled(query),
            Err(close) => Self::Unsettled(io::Error::new(
                close.kind(),
                DnsQuicCleanupFailure {
                    query: query.err(),
                    close,
                },
            )),
        }
    }
}
/// Actual standalone QUIC query and endpoint-cleanup errors. This diagnostic
/// grants no retry, registration ACK or native-close authority.
#[derive(Debug)]
pub struct DnsQuicCleanupFailure {
    query: Option<io::Error>,
    close: io::Error,
}
impl DnsQuicCleanupFailure {
    pub fn query_error(&self) -> Option<&io::Error> {
        self.query.as_ref()
    }
    pub fn close_error(&self) -> &io::Error {
        &self.close
    }
}
impl std::fmt::Display for DnsQuicCleanupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(query) = &self.query {
            write!(f, "QUIC DNS query failed: {query}; ")?;
        }
        write!(
            f,
            "original QUIC DNS endpoint cleanup failed: {}",
            self.close
        )
    }
}
impl std::error::Error for DnsQuicCleanupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.close)
    }
}
