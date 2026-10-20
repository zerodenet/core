//! Opt-in hooks for a caller that already owns every original native socket.
//! These hooks never create, register, clone or close a socket. Legacy entry
//! points keep their existing probe and error-handling behavior.
use crate::{Transmit, UdpSockRef};
use std::io;

/// The native option queried by the existing platform capability algorithm.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProbeKind {
    Gso,
    Gro,
}
/// The implementation owns each probe before creating its native handle and
/// confirms its closure before returning or attempting a fallback address.
pub trait NativeProbes {
    fn detect(&mut self, kind: ProbeKind) -> io::Result<usize>;
}
/// A platform fact that needs no probe socket, or None when a real probe is
/// required. The caller must not invent a smaller capability on budget failure.
pub fn capability_without_probe(kind: ProbeKind) -> Option<usize> {
    crate::imp::capability_without_probe(kind)
}
/// Run the existing option test on an already owned probe. This borrows the
/// exact original; it never allocates a hidden fallback descriptor. Native
/// option errors are exposed so its owner can preserve their original source.
pub fn probe_owned_socket(kind: ProbeKind, socket: UdpSockRef<'_>) -> io::Result<usize> {
    crate::imp::probe_owned_socket(kind, socket)
}
/// Observed syscall submission, never a remote delivery acknowledgment.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SendOutcome {
    pub bytes: usize,
    pub datagrams: usize,
    /// False means only the observed prefix was submitted; callers must not
    /// retry the complete original contents and duplicate that prefix.
    pub complete: bool,
}
impl SendOutcome {
    #[cfg(not(apple_fast))]
    pub(crate) fn bytes(transmit: &Transmit<'_>, bytes: usize) -> Self {
        let stride = transmit
            .segment_size
            .unwrap_or(transmit.contents.len())
            .max(1);
        Self {
            bytes,
            datagrams: if transmit.contents.is_empty() {
                1
            } else {
                bytes.div_ceil(stride)
            },
            complete: bytes == transmit.contents.len(),
        }
    }
    #[cfg(apple_fast)]
    pub(crate) fn prefix(transmit: &Transmit<'_>, messages: usize) -> Self {
        let stride = transmit
            .segment_size
            .unwrap_or(transmit.contents.len())
            .max(1);
        let expected = transmit.contents.len().div_ceil(stride).max(1);
        let submitted = messages.min(expected);
        Self {
            bytes: submitted
                .saturating_mul(stride)
                .min(transmit.contents.len()),
            datagrams: submitted,
            complete: messages == expected,
        }
    }
}
