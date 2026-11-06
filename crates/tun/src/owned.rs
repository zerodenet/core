//! Explicit closure of the exact handle owned by this TUN instance.
use std::{
    io,
    sync::{Arc, OnceLock},
};

/// Sealed identity of one actually owned backend handle, independent of the
/// interface name and of descriptor integers that an OS may later reuse.
#[derive(Clone, Debug)]
pub struct OwnedTunIdentity {
    inner: Arc<HandleIdentity>,
}
#[derive(Debug)]
struct HandleIdentity {
    closed: OnceLock<Arc<NativeClosed>>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    descriptor: std::os::fd::RawFd,
    #[cfg(target_os = "windows")]
    session: wintun::SessionIdentity,
    #[cfg(target_os = "windows")]
    adapter: wintun::AdapterIdentity,
}
impl OwnedTunIdentity {
    #[cfg(target_os = "windows")]
    pub(crate) fn from_wintun(
        session: wintun::SessionIdentity,
        adapter: wintun::AdapterIdentity,
    ) -> Self {
        Self {
            inner: Arc::new(HandleIdentity {
                closed: OnceLock::new(),
                session,
                adapter,
            }),
        }
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub(crate) fn from_fd(descriptor: std::os::fd::RawFd) -> Self {
        Self {
            inner: Arc::new(HandleIdentity {
                closed: OnceLock::new(),
                descriptor,
            }),
        }
    }
    /// A read-only matching observation, written only by successful explicit
    /// native close. This retains no native handle, controller or task owner.
    pub fn closed(&self) -> Option<OwnedTunClosed> {
        Some(OwnedTunClosed {
            identity: self.clone(),
            native: self.inner.closed.get()?.clone(),
        })
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

// The stored fact deliberately contains no OwnedTunIdentity/receipt: a receipt
// owns its identity, so storing that receipt in the identity would form a cycle.
#[derive(Debug)]
struct NativeClosed {
    interface: String,
    #[cfg(target_os = "windows")]
    session: Arc<wintun::EndSessionReceipt>,
    #[cfg(target_os = "windows")]
    adapter: wintun::CloseAdapterReceipt,
}
/// Sealed evidence for this original native handle only. Unique view return,
/// native worker/Scope joins and route recovery are independent obligations.
#[derive(Clone, Debug)]
pub struct OwnedTunClosed {
    identity: OwnedTunIdentity,
    native: Arc<NativeClosed>,
}
impl OwnedTunClosed {
    pub fn matches(&self, identity: &OwnedTunIdentity) -> bool {
        self.identity.same_owner(identity)
            && identity
                .inner
                .closed
                .get()
                .is_some_and(|native| Arc::ptr_eq(native, &self.native))
    }
    pub fn interface(&self) -> &str {
        &self.native.interface
    }
    #[cfg(target_os = "windows")]
    pub fn ended_session(&self) -> &wintun::EndSessionReceipt {
        &self.native.session
    }
    #[cfg(target_os = "windows")]
    pub fn closed_adapter(&self) -> &wintun::CloseAdapterReceipt {
        &self.native.adapter
    }
}

/// An explicit backend close result. It certifies neither other descriptor
/// owners nor routes, in-flight users, task joins or a host-owned interface.
#[derive(Clone, Debug)]
pub struct TunCloseReceipt {
    interface: String,
    identity: OwnedTunIdentity,
    #[cfg(target_os = "windows")]
    session: Arc<wintun::EndSessionReceipt>,
    #[cfg(target_os = "windows")]
    adapter: wintun::CloseAdapterReceipt,
}
impl TunCloseReceipt {
    #[cfg(target_os = "windows")]
    pub fn ended_session(&self) -> &wintun::EndSessionReceipt {
        &self.session
    }
    #[cfg(target_os = "windows")]
    pub fn closed_adapter(&self) -> &wintun::CloseAdapterReceipt {
        &self.adapter
    }
    #[cfg(target_os = "windows")]
    pub(crate) fn from_wintun(
        interface: String,
        identity: OwnedTunIdentity,
        session: Arc<wintun::EndSessionReceipt>,
        adapter: wintun::CloseAdapterReceipt,
    ) -> io::Result<Self> {
        if !session.identity().same_owner(&identity.inner.session)
            || !adapter.identity().same_owner(&identity.inner.adapter)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Wintun original close identities differ",
            ));
        }
        let _ = identity.inner.closed.set(Arc::new(NativeClosed {
            interface: interface.clone(),
            session: session.clone(),
            adapter: adapter.clone(),
        }));
        Ok(Self {
            interface,
            identity,
            session,
            adapter,
        })
    }
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
    let _ = identity.inner.closed.set(Arc::new(NativeClosed {
        interface: interface.clone(),
    }));
    Ok(TunCloseReceipt {
        interface,
        identity,
    })
}

#[cfg(all(test, any(target_os = "linux", target_os = "macos")))]
mod tests;
