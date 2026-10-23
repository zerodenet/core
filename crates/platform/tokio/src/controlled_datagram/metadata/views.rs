//! Exact issued I/O views. These receipts do not prove logical user release,
//! task join/reception, native close, or complete protocol resource coverage.
use super::*;
use crate::contract::scope::{TaskRegistrar, TaskScopeIdentity};
use std::sync::Weak;
#[derive(Clone, Copy)]
pub(super) enum ViewKind {
    Receiver,
    Sender,
    Poller,
}
pub(super) struct Views(Mutex<[usize; 3]>);
impl Views {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self(Mutex::new([0; 3])))
    }
    fn acquire(self: &Arc<Self>, kind: ViewKind) -> ViewToken {
        let index = kind as usize;
        let mut counts = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        counts[index] = counts[index]
            .checked_add(1)
            .expect("issued metadata view identity capacity exhausted");
        ViewToken {
            views: self.clone(),
            kind,
        }
    }
    fn empty(&self) -> bool {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            == [0; 3]
    }
}
pub(super) struct ViewToken {
    views: Arc<Views>,
    kind: ViewKind,
}
impl ViewToken {
    pub(super) fn new(views: &Arc<Views>, kind: ViewKind) -> Self {
        views.acquire(kind)
    }
}
impl Clone for ViewToken {
    fn clone(&self) -> Self {
        self.views.acquire(self.kind)
    }
}
impl Drop for ViewToken {
    fn drop(&mut self) {
        let mut counts = self
            .views
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let index = self.kind as usize;
        counts[index] = counts[index]
            .checked_sub(1)
            .expect("issued metadata view released exactly once");
    }
}
pub(super) struct Claim {
    original: Weak<Shared>,
    pub(super) scope: TaskScopeIdentity,
}
/// Host-retained validation authority for precisely one original and scope.
/// It retains neither the native original, task scope, protocol nor runtime.
pub struct MetadataViewIssuer(Arc<Claim>);
/// Unique claim handed to the consuming owner. No public constructor/fields.
pub struct MetadataViewClaim(pub(super) Arc<Claim>);
/// Immutable historical fact: every actual issued native I/O view was released.
/// It says nothing about logical guards, native closure, or task delivery.
#[derive(Clone)]
pub struct MetadataViewsClosedReceipt(pub(super) Arc<Claim>);
impl MetadataDatagramControl {
    /// Capture identity only; no native I/O or task registration is granted.
    /// The host validates its real StartAccess/open scope separately.
    pub fn claim_views(
        &mut self,
        scope: TaskScopeIdentity,
    ) -> std_io::Result<(MetadataViewIssuer, MetadataViewClaim)> {
        self.shared.check()?;
        if self.view_claim.is_some() {
            return Err(std_io::Error::new(
                std_io::ErrorKind::AlreadyExists,
                "metadata view claim was already issued",
            ));
        }
        let claim = Arc::new(Claim {
            original: Arc::downgrade(&self.shared),
            scope,
        });
        self.view_claim = Some(claim.clone());
        Ok((MetadataViewIssuer(claim.clone()), MetadataViewClaim(claim)))
    }
}
impl MetadataViewClaim {
    /// Verify the exact original views before consuming this unique claim into
    /// a sealed user graph. This does not create a view or grant native control.
    pub fn matches(&self, rx: &MetadataDatagramRx, tx: &MetadataDatagramTx) -> bool {
        let original = self.0.original.clone();
        original.ptr_eq(&Arc::downgrade(&rx.shared)) && original.ptr_eq(&Arc::downgrade(&tx.shared))
    }
    pub fn try_closed(&self) -> Option<MetadataViewsClosedReceipt> {
        let original = self.0.original.upgrade()?;
        if !original.views.empty() {
            return None;
        }
        // Once all original RX/TX/pollers are gone there is no view from which
        // another clone or poller can be created. New binds get new originals.
        Some(MetadataViewsClosedReceipt(self.0.clone()))
    }
}
impl MetadataViewIssuer {
    /// Users/driver proof from the unique graph bound to this exact original
    /// claim. Native FD closure remains independently owned and verified.
    pub fn validates_users(
        &self,
        receipt: &users::MetadataUsersClosedReceipt,
        tasks: &TaskRegistrar,
    ) -> bool {
        self.validates(&receipt.views, tasks)
    }
    /// Validate only this opaque view receipt. The host separately requires
    /// logical users and actual task joins/reception before whole-graph closure.
    pub fn validates(&self, receipt: &MetadataViewsClosedReceipt, tasks: &TaskRegistrar) -> bool {
        Arc::ptr_eq(&self.0, &receipt.0) && self.0.scope.matches(tasks)
    }
}
impl fmt::Debug for MetadataViewsClosedReceipt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataViewsClosedReceipt")
            .finish_non_exhaustive()
    }
}
