use super::{
    error,
    owner::Shared,
    slot::{Slot, State},
};
use crate::contract::{ChannelError, DeliveryState, Reason, Stage};
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use zero_core::component::ComponentInstanceId;
/// Cloneable finite admission; it cannot declare native I/O closed.
#[derive(Clone)]
pub struct CarrierGrants(pub(super) Arc<Shared>);
pub struct CarrierGrant {
    pub(super) slot: Arc<Slot>,
    pub(super) transferred: bool,
}
impl CarrierGrants {
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.instance
    }
    pub fn reserve(&self) -> Result<CarrierGrant, ChannelError> {
        let mut state = self.0.state();
        if !state.live
            || state.stopped
            || self.0.cancel.is_cancelled()
            || state
                .binding
                .as_ref()
                .is_some_and(|bound| bound.host_cancelled())
        {
            return Err(error(
                self.0.instance,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ));
        }
        if state.slots.len() >= self.0.limit.get() {
            return Err(error(
                self.0.instance,
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            ));
        }
        let next = state.next.checked_add(1).ok_or_else(|| {
            error(
                self.0.instance,
                Stage::Prepare,
                Reason::Capacity,
                DeliveryState::NotSubmitted,
            )
        })?;
        state.next = next;
        let slot = Arc::new(Slot {
            instance: self.0.instance,
            ordinal: next,
            owner: Arc::downgrade(&self.0),
            cancel: self.0.cancel.child_token(),
            state: Mutex::new(State::default()),
            changed: Notify::new(),
            closing: tokio::sync::Mutex::new(()),
        });
        state.slots.push(slot.clone());
        Ok(CarrierGrant {
            slot,
            transferred: false,
        })
    }
}
impl CarrierGrant {
    pub fn instance(&self) -> ComponentInstanceId {
        self.slot.instance
    }
    /// Associates a persistent caller record with this exact reservation before
    /// native preparation or its first await. This is neither readiness nor a
    /// close/join receipt; spawning still requires a same-slot `CarrierDriver`.
    pub fn adoption(&self) -> super::CarrierAdoption {
        super::CarrierAdoption {
            slot: self.slot.clone(),
        }
    }
    pub(super) fn check(&self) -> Result<(), ChannelError> {
        if self.slot.cancel.is_cancelled() {
            Err(error(
                self.slot.instance,
                Stage::Prepare,
                Reason::Revoked,
                DeliveryState::NotSubmitted,
            ))
        } else {
            Ok(())
        }
    }
}
impl Drop for CarrierGrant {
    fn drop(&mut self) {
        if !self.transferred {
            self.slot.state().opening = false;
            self.slot.unused();
        }
    }
}
