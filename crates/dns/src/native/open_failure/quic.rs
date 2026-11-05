//! Only the original Metadata provider can supply these sealed receipts.
use std::{fmt, io};
use zero_platform_tokio::controlled_datagram::metadata_pool::{
    MetadataPoolClosed, MetadataPoolOrigin,
};

pub struct NativeDnsQuicOpenFailure {
    opening: io::Error,
    original: MetadataPoolOrigin,
    closed: MetadataPoolClosed,
}
impl NativeDnsQuicOpenFailure {
    pub fn after_close(
        opening: io::Error,
        original: MetadataPoolOrigin,
        closed: MetadataPoolClosed,
    ) -> io::Error {
        if !closed.matches(&original) || !closed.had_native_acquisition() {
            return opening;
        }
        io::Error::new(
            opening.kind(),
            Self {
                opening,
                original,
                closed,
            },
        )
    }
    pub fn original(&self) -> &MetadataPoolOrigin {
        &self.original
    }
    pub fn original_close(&self) -> &MetadataPoolClosed {
        &self.closed
    }
}
impl fmt::Debug for NativeDnsQuicOpenFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeDnsQuicOpenFailure")
            .field("opening", &self.opening)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for NativeDnsQuicOpenFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.opening.fmt(f)
    }
}
impl std::error::Error for NativeDnsQuicOpenFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.opening)
    }
}
