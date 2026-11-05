//! An open error can carry only the original controller's completed cleanup.
use std::{fmt, io};
use zero_platform_tokio::controlled_carrier::{CarrierAdoption, CarrierClosed};

/// Preserves the original opening error and a confirmed native-root close.
/// Error kinds, cancelled futures and unused reservations cannot mint this.
pub struct NativeDnsOpenFailure {
    opening: io::Error,
    closed: CarrierClosed,
}
impl NativeDnsOpenFailure {
    pub fn after_close(opening: io::Error, original: &CarrierAdoption) -> io::Error {
        let closed = original.closed().filter(|closed| {
            closed.matches(original)
                && closed.had_native_acquisition()
                && closed.borrowed_origin().is_none()
                && original
                    .users_closed()
                    .is_some_and(|users| users.matches(original))
        });
        match closed {
            Some(closed) => io::Error::new(opening.kind(), Self { opening, closed }),
            None => opening,
        }
    }
    pub fn opening_error(&self) -> &io::Error {
        &self.opening
    }
    pub fn original_close(&self) -> &CarrierClosed {
        &self.closed
    }
}
impl fmt::Debug for NativeDnsOpenFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeDnsOpenFailure")
            .field("opening", &self.opening)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for NativeDnsOpenFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.opening, f)
    }
}
impl std::error::Error for NativeDnsOpenFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.opening)
    }
}

#[cfg(feature = "doq")]
mod quic;
#[cfg(feature = "doq")]
pub use quic::NativeDnsQuicOpenFailure;
