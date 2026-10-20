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
    /// Weak observation of this existing capability's same original owner.
    /// The observation cannot reconstruct admission or native permissions.
    pub fn registration(&self) -> super::CarrierOwnerRegistration {
        super::CarrierOwnerRegistration::new(&self.0)
    }
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

impl CarrierGrants {
    /// Reclaim only original slots whose actual views/callers/tasks have already
    /// returned. This is an inline maintenance step, never a generation-close
    /// receipt, cancellation request for active users, or extra background task.
    pub async fn reap_returned(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<usize, ChannelError> {
        let slots = { self.0.state().slots.clone() };
        let mut retired = 0;
        for slot in slots {
            let (idle, users, driver, auxiliary) = {
                let state = slot.state();
                (
                    !state.opening
                        && !state.spawning
                        && !state.view_live
                        && state.unknown.is_none(),
                    state.users.clone(),
                    state.driver.clone(),
                    state.auxiliary.clone(),
                )
            };
            // Witness getters and their last error/source drops are outside the
            // actual carrier state lock. The close transaction rechecks state.
            if !idle
                || users.is_some_and(|users| !users.ready())
                || driver.is_some_and(|(_, witness)| !witness.is_joined())
                || auxiliary.is_some_and(|witness| !witness.is_joined())
            {
                continue;
            }
            let report = super::owner::close_slot(&slot, deadline).await?;
            if report.is_complete() {
                self.0.state().confirmed.append(report);
                retired += 1;
            }
        }
        Ok(retired)
    }
}
