//! Proven deadline observations from this resolver's own cleanup waiters.
//! This payload is not created from an upstream error's classification fields.
use std::{error::Error, fmt, io};
/// Only this crate's held `timeout_at` Err constructs this observation. It is
/// not IO/native/task closure and cannot release a resource or registration.
/// Receivers inspect this top-level payload only, never an arbitrary source
/// chain which may also contain an independent real work/cleanup failure.
#[derive(Debug)]
pub struct DnsRetirementPending {
    actual_deadline: tokio::time::error::Elapsed,
    message: &'static str,
}
impl fmt::Display for DnsRetirementPending {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            self.actual_deadline.fmt(formatter)
        } else {
            formatter.write_str(self.message)
        }
    }
}
impl Error for DnsRetirementPending {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.actual_deadline)
    }
}
pub(crate) fn deadline(
    actual_deadline: tokio::time::error::Elapsed,
    message: &'static str,
) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        DnsRetirementPending {
            actual_deadline,
            message,
        },
    )
}
#[cfg(test)]
#[path = "tests/retirement_observation.rs"]
mod tests;
