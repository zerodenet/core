//! Read-only lineage of the actual native carrier and its optional real driver.
use super::{error, slot::Slot, CarrierAdoption, CarrierOwnerRegistration};
use crate::contract::{scope::TaskJoinWitness, ChannelError, DeliveryState, Reason, Stage};
use std::sync::{Arc, Weak};
use zero_core::component::ComponentInstanceId;

/// A sealed shared observation, never native I/O or shutdown authority.
/// Erasure here also keeps recursive Slot/LoanOwner internals out of upstream
/// Send/Sync inference and makes per-session clones one reference-count change.
#[derive(Clone)]
pub struct CarrierOrigin(Arc<dyn Original>);
trait Original: Send + Sync {
    fn slot(&self) -> &Weak<Slot>;
    fn owner(&self) -> &CarrierOwnerRegistration;
    fn driver(&self) -> Option<&TaskJoinWitness>;
    fn parent(&self) -> Option<&CarrierOrigin>;
    fn completion(&self) -> &Arc<std::sync::OnceLock<bool>>;
}
struct ActualOriginal {
    slot: Weak<Slot>,
    owner: CarrierOwnerRegistration,
    driver: Option<TaskJoinWitness>,
    parent: Option<CarrierOrigin>,
    completion: Arc<std::sync::OnceLock<bool>>,
}
impl Original for ActualOriginal {
    fn slot(&self) -> &Weak<Slot> {
        &self.slot
    }
    fn owner(&self) -> &CarrierOwnerRegistration {
        &self.owner
    }
    fn driver(&self) -> Option<&TaskJoinWitness> {
        self.driver.as_ref()
    }
    fn parent(&self) -> Option<&CarrierOrigin> {
        self.parent.as_ref()
    }
    fn completion(&self) -> &Arc<std::sync::OnceLock<bool>> {
        &self.completion
    }
}
impl CarrierAdoption {
    /// Capture only the driver actually registered by this original slot. A
    /// witness from another slot/scope, or an unused grant, cannot name a donor.
    pub fn origin(&self, driver: &TaskJoinWitness) -> Result<CarrierOrigin, ChannelError> {
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| self.origin_error())?;
        let valid = {
            let state = self.slot.state();
            !state.closed
                && (state.control.is_some() || state.packet.is_some() || state.loan.is_some())
                && state
                    .driver
                    .as_ref()
                    .is_some_and(|(_, actual)| actual.same_registration(driver))
        };
        if !valid {
            return Err(self.origin_error());
        }
        Ok(CarrierOrigin(Arc::new(ActualOriginal {
            completion: self.slot.completion.clone(),
            slot: Arc::downgrade(&self.slot),
            owner: CarrierOwnerRegistration::new(&owner),
            driver: Some(driver.clone()),
            parent: self.slot.state().loan_origin.clone(),
        })))
    }
    pub(super) fn transferred_origin(&self) -> Result<CarrierOrigin, ChannelError> {
        let owner = self
            .slot
            .owner
            .upgrade()
            .ok_or_else(|| self.origin_error())?;
        let mut state = self.slot.state();
        if state.closed
            || state.spawn_attempted
            || (state.control.is_none() && state.packet.is_none())
            || state.loan.is_some()
            || !state
                .users
                .as_ref()
                .is_some_and(|users| users.transferred())
        {
            return Err(self.origin_error());
        }
        state.transferred_origin = true;
        Ok(CarrierOrigin(Arc::new(ActualOriginal {
            slot: Arc::downgrade(&self.slot),
            owner: CarrierOwnerRegistration::new(&owner),
            driver: None,
            parent: None,
            completion: self.slot.completion.clone(),
        })))
    }
    fn origin_error(&self) -> ChannelError {
        error(
            self.instance(),
            Stage::Prepare,
            Reason::StalePlan,
            DeliveryState::NotSubmitted,
        )
    }
}
impl CarrierOrigin {
    /// Same original-slot disposition, retained as metadata after the actual
    /// provider and transferred views have been confirmed and removed.
    pub fn closed(&self) -> Option<CarrierOriginClosed> {
        self.0.completion().get().map(|_| CarrierOriginClosed {
            completion: self.0.completion().clone(),
        })
    }
    pub fn parent(&self) -> Option<&Self> {
        self.0.parent()
    }
    pub fn native_origin(&self) -> &Self {
        let mut original = self;
        while let Some(parent) = original.parent() {
            original = parent;
        }
        original
    }
    pub fn endpoints(&self) -> Option<super::CarrierEndpoints> {
        self.native_origin()
            .0
            .slot()
            .upgrade()?
            .state()
            .endpoints
            .clone()
    }
    pub fn selected_binding(
        &self,
    ) -> Option<(
        zero_core::request::Destination,
        crate::contract::network::UpstreamBinding,
    )> {
        let slot = self.0.slot().upgrade()?;
        let state = slot.state();
        Some((state.endpoint.clone()?, state.binding.clone()?))
    }
    pub fn instance(&self) -> ComponentInstanceId {
        self.0.owner().instance()
    }
    pub fn owner(&self) -> &CarrierOwnerRegistration {
        self.0.owner()
    }
    /// None identifies taskless custody; it is never an exit or close receipt.
    pub fn driver(&self) -> Option<&TaskJoinWitness> {
        self.0.driver()
    }
    pub fn same_original(&self, other: &Self) -> bool {
        Weak::ptr_eq(self.0.slot(), other.0.slot())
            && match (self.driver(), other.driver()) {
                (Some(a), Some(b)) => a.same_registration(b),
                (None, None) => true,
                _ => false,
            }
            && self.owner().same_owner(other.owner())
    }
    pub fn is_retired(&self) -> bool {
        let mut current = Some(self);
        while let Some(original) = current {
            if original
                .0
                .slot()
                .upgrade()
                .is_none_or(|slot| slot.cancel.is_cancelled())
            {
                return true;
            }
            current = original.parent();
        }
        false
    }
    /// Cancellation of the actual original, with no authority to stop it.
    pub async fn retired(&self) {
        use std::future::Future;
        let mut current = Some(self);
        let mut waits = Vec::new();
        while let Some(original) = current {
            let Some(slot) = original.0.slot().upgrade() else {
                return;
            };
            waits.push(Box::pin(slot.cancel.clone().cancelled_owned()));
            current = original.parent();
        }
        std::future::poll_fn(move |cx| {
            for wait in &mut waits {
                if wait.as_mut().poll(cx).is_ready() {
                    return std::task::Poll::Ready(());
                }
            }
            std::task::Poll::Pending
        })
        .await;
    }
}
#[derive(Clone)]
pub struct CarrierOriginClosed {
    completion: Arc<std::sync::OnceLock<bool>>,
}
impl CarrierOriginClosed {
    pub fn matches(&self, origin: &CarrierOrigin) -> bool {
        Arc::ptr_eq(&self.completion, origin.0.completion()) && self.completion.get().is_some()
    }
    /// Borrowed originals settle their logical control; only the native root
    /// can certify acquisition and closure of its own actual socket.
    pub fn had_native_acquisition(&self) -> bool {
        self.completion.get() == Some(&true)
    }
}
