use std::{fmt, sync::Arc};
/// Original H3 lifecycle result, separate from the actual host TaskJoined.
/// Cancellation/handoff loss never stands in for native socket closure.
#[derive(Debug)]
pub enum Http3DriverExit {
    Handshake(Arc<::h3::error::ConnectionError>),
    Closed(Arc<::h3::error::ConnectionError>),
    Cancelled,
    HandoffDropped,
}
/// The same actual terminal Arc retained in the driver's original body record.
#[derive(Debug)]
pub struct Http3DriverFailure(pub Arc<Http3DriverExit>);
impl fmt::Display for Http3DriverFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_ref() {
            Http3DriverExit::Handshake(source) | Http3DriverExit::Closed(source) => source.fmt(f),
            Http3DriverExit::Cancelled => f.write_str("original H3 owner cancelled"),
            Http3DriverExit::HandoffDropped => f.write_str("original H3 sender receiver dropped"),
        }
    }
}
impl std::error::Error for Http3DriverFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.0.as_ref() {
            // Pinned h3 intentionally keeps its nested variants opaque unless
            // its breaking third-party-backend feature is opted into. Preserve
            // this same complete typed terminal Arc, without enabling another
            // feature, reconstructing its text, or replacing the Quinn ledger's
            // separately retained original native/connection sources.
            Http3DriverExit::Handshake(source) | Http3DriverExit::Closed(source) => {
                Some(source.as_ref())
            }
            _ => None,
        }
    }
}
