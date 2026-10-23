//! A failed Darwin temporary interface restoration cannot authorize replay.
use std::{fmt, io};

/// The synchronous send was attempted, but its interface scope could not be
/// restored. The enclosing io::Error is always Other, including when the OS
/// restoration error is WouldBlock: retry could duplicate an actual submission.
/// Both original syscall outcomes remain available without string inspection.
#[derive(Debug)]
pub struct PacketScopeRestoreFailure {
    send: io::Result<usize>,
    restore: io::Error,
}
impl PacketScopeRestoreFailure {
    #[cfg(any(target_os = "macos", test))]
    pub(crate) fn new(send: io::Result<usize>, restore: io::Error) -> Self {
        Self { send, restore }
    }
    /// Recover structured facts from either the original error or the same
    /// error retained by the controlled listener. No message matching is used.
    pub fn from_io(mut error: &io::Error) -> Option<&Self> {
        loop {
            let cause = error.get_ref()?;
            if let Some(failure) = cause.downcast_ref::<Self>() {
                return Some(failure);
            }
            error = cause.downcast_ref::<std::sync::Arc<io::Error>>()?.as_ref();
        }
    }
    /// A successful syscall means submission, not remote receipt/delivery.
    pub fn submitted_bytes(&self) -> Option<usize> {
        self.send.as_ref().ok().copied()
    }
    pub fn send_error(&self) -> Option<&io::Error> {
        self.send.as_ref().err()
    }
    pub fn restoration_error(&self) -> &io::Error {
        &self.restore
    }
}
impl fmt::Display for PacketScopeRestoreFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "UDP interface restoration after send failed: {}",
            self.restore
        )
    }
}
impl std::error::Error for PacketScopeRestoreFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.restore)
    }
}

#[cfg(test)]
#[path = "../../tests/packet_socket/failure.rs"]
mod tests;
