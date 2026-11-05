//! Matching evidence for one original adapter handle, separate from sessions.
use super::*;

#[derive(Clone, Debug)]
pub struct AdapterIdentity(Arc<Identity>);
#[derive(Debug)]
struct Identity {
    native: usize,
}
impl AdapterIdentity {
    pub(super) fn new(native: usize) -> Self {
        Self(Arc::new(Identity { native }))
    }
    pub fn same_owner(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
/// WintunCloseAdapter returned for this owned handle. Wintun exposes no error
/// return. This certifies neither host interface deletion nor session/thread joins.
#[derive(Clone, Debug)]
pub struct CloseAdapterReceipt {
    identity: AdapterIdentity,
}
impl CloseAdapterReceipt {
    pub fn identity(&self) -> &AdapterIdentity {
        &self.identity
    }
}
impl Adapter {
    pub fn owned_identity(&self) -> AdapterIdentity {
        self.identity.clone()
    }
    /// Consuming unique ownership is possible only after every active Session
    /// and other adapter user released its Arc. The host must join readers apart.
    pub fn close_owned(mut self) -> CloseAdapterReceipt {
        assert_eq!(self.identity.0.native, self.adapter.0 as usize);
        let original = std::mem::replace(&mut self.adapter.0, ptr::null_mut());
        unsafe { self.wintun.WintunCloseAdapter(original) };
        CloseAdapterReceipt {
            identity: self.identity.clone(),
        }
    }
}
#[derive(Debug)]
pub struct AdapterOpenError {
    closed: CloseAdapterReceipt,
    source: Error,
}
impl AdapterOpenError {
    pub(super) fn new(closed: CloseAdapterReceipt, source: Error) -> Self {
        Self { closed, source }
    }
    pub fn closed(&self) -> &CloseAdapterReceipt {
        &self.closed
    }
}
impl std::fmt::Display for AdapterOpenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "adapter discovery failed after closing its original: {}",
            self.source
        )
    }
}
impl std::error::Error for AdapterOpenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}
