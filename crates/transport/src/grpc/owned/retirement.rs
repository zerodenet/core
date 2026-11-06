//! Compact heavy material only after the real owner's complete settlement.
use super::*;
use crate::grpc::{pool::State, GrpcPool};
use std::sync::Arc;
use zero_platform_tokio::controlled_carrier::{CarrierOwnerClosed, CarrierOwnerRegistration};
/// Private issuance requires matching native/users/logical/actual task ACKs.
/// This retains no task registrar, protocol options, CA bytes or live original.
pub(in crate::grpc) struct Retired {
    owner: CarrierOwnerRegistration,
    closed: CarrierOwnerClosed,
    selected_background: bool,
}
impl Retired {
    pub(in crate::grpc) fn permits_background(&self) -> bool {
        self.closed.matches(&self.owner)
    }
    pub(in crate::grpc) fn permits_legacy(&self) -> bool {
        self.selected_background && self.permits_background()
    }
}
impl State {
    pub(super) fn compact_closed_owner(&self) -> bool {
        let old = match &*self.ownership.lock().unwrap() {
            Ownership::Managed(owner) => owner.clone(),
            Ownership::Retired(receipt) => return receipt.permits_background(),
            _ => return false,
        };
        if !old.ready_to_replace() {
            return false;
        }
        let owner = old.grants.registration();
        let Some(closed) = owner.closed().filter(|receipt| receipt.matches(&owner)) else {
            return false;
        };
        let receipt = Retired {
            owner,
            closed,
            selected_background: old.options.selected_background,
        };
        let previous = {
            let mut state = self.ownership.lock().unwrap();
            if !matches!(&*state,Ownership::Managed(actual) if Arc::ptr_eq(actual,&old)) {
                return false;
            }
            std::mem::replace(&mut *state, Ownership::Retired(receipt))
        };
        // Destruction is metadata/material reclamation after the above proofs;
        // it never supplies closure or clears a legacy exposure.
        drop(previous);
        drop(old);
        true
    }
}
impl GrpcPool {
    /// Reclaim closed owner material; a positive actual receipt is mandatory.
    /// The exact last-good component recipe retains its own recovery snapshot.
    pub fn compact_closed_owner(&self) -> bool {
        self.0.compact_closed_owner()
    }
}
