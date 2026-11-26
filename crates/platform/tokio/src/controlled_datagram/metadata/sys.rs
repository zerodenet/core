//! First-party controlled UDP ancillary socket state (ECN, PKTINFO, GSO, GRO).
//!
//! Every native socket — including capability probes — is owned by the caller's
//! `ControlledDatagramOwner` before creation and confirmed closed before fallback.

use std::io;

#[cfg(any(unix, windows))]
mod cmsg;

#[cfg(unix)]
mod unix;
#[cfg(unix)]
pub(super) use unix::{UdpSocketState, BATCH_SIZE};

#[cfg(windows)]
mod windows;
#[cfg(windows)]
pub(super) use windows::{UdpSocketState, BATCH_SIZE};

#[cfg(not(any(unix, windows)))]
mod fallback;
#[cfg(not(any(unix, windows)))]
pub(super) use fallback::{UdpSocketState, BATCH_SIZE};

/// Native socket offload capability queried during `MetadataOwner` initialization.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProbeKind {
    Gso,
    #[cfg_attr(windows, allow(dead_code))]
    Gro,
}

/// Caller-owned probe factory that allocates and closes each temporary probe socket
/// inside its persistent `ControlledDatagramOwner`.
pub(super) trait NativeProbes {
    fn detect(&mut self, kind: ProbeKind) -> io::Result<usize>;
}

/// Returns a static platform capability when no live probe socket is required.
pub(super) fn capability_without_probe(kind: ProbeKind) -> Option<usize> {
    #[cfg(unix)]
    {
        unix::capability_without_probe(kind)
    }
    #[cfg(windows)]
    {
        windows::capability_without_probe(kind)
    }
    #[cfg(not(any(unix, windows)))]
    {
        fallback::capability_without_probe(kind)
    }
}

/// Executes the native socket option probe on an already-owned raw probe socket.
pub(super) fn probe_owned_socket(
    kind: ProbeKind,
    socket: socket2::SockRef<'_>,
) -> io::Result<usize> {
    #[cfg(unix)]
    {
        unix::probe_owned_socket(kind, socket)
    }
    #[cfg(windows)]
    {
        windows::probe_owned_socket(kind, socket)
    }
    #[cfg(not(any(unix, windows)))]
    {
        fallback::probe_owned_socket(kind, socket)
    }
}
