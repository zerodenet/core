//! Explicit closure of the exact handle owned by this TUN instance.
use std::{io, sync::Arc};

/// Sealed identity of one actually owned backend handle, independent of the
/// interface name and of descriptor integers that an OS may later reuse.
#[derive(Clone, Debug)]
pub struct OwnedTunIdentity {
    inner: Arc<HandleIdentity>,
}
#[derive(Debug)]
struct HandleIdentity {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    descriptor: std::os::fd::RawFd,
}
impl OwnedTunIdentity {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn from_fd(descriptor: std::os::fd::RawFd) -> Self {
        Self {
            inner: Arc::new(HandleIdentity { descriptor }),
        }
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

/// An explicit backend close result. It certifies neither other descriptor
/// owners nor routes, in-flight users, task joins or a host-owned interface.
#[derive(Debug)]
pub struct TunCloseReceipt {
    interface: String,
    identity: OwnedTunIdentity,
}
impl TunCloseReceipt {
    pub fn identity(&self) -> &OwnedTunIdentity {
        &self.identity
    }
    pub fn interface(&self) -> &str {
        &self.interface
    }
}

/// A backend that can explicitly consume and close its original owned handle.
///
/// Callers must first stop and join all tasks borrowing this instance. There is
/// no default implementation: dropping a device cannot issue a close receipt.
pub trait OwnedTunDevice: crate::TunDevice + Sized {
    fn owned_identity(&self) -> OwnedTunIdentity;
    fn close_owned(self) -> io::Result<TunCloseReceipt>;
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
pub(crate) fn close_fd(
    fd: impl std::os::fd::IntoRawFd + std::os::fd::AsRawFd,
    interface: String,
    identity: OwnedTunIdentity,
) -> io::Result<TunCloseReceipt> {
    if fd.as_raw_fd() != identity.inner.descriptor {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "owned TUN handle identity mismatch",
        ));
    }
    let fd = fd.into_raw_fd();
    // SAFETY: ownership was consumed from the original File/OwnedFd after its
    // async registration was removed; no Rust owner may close it a second time.
    // In particular, adopt() only passes Zero's duplicated descriptor here.
    if unsafe { libc::close(fd) } != 0 {
        // Do not retry EINTR: on some hosts the descriptor has already closed
        // and another thread may have reused its integer. Failure stays unknown
        // with the original OS error; it never produces a successful receipt.
        return Err(io::Error::last_os_error());
    }
    Ok(TunCloseReceipt {
        interface,
        identity,
    })
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
