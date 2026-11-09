//! Sealed custody of an original in its actual host pool and driver Scope.
use super::{
    binding::BoundAuthority, owner::Shared, slot::Slot, CarrierAdoption, CarrierOwnerRegistration,
};
use crate::contract::scope::TaskJoinWitness;
use std::sync::{Arc, Weak};

/// Observation only: the actual host still retains this exact original and
/// registered driver. This neither authorizes I/O nor reports native closure.
#[derive(Clone)]
pub struct CarrierDriverRetained {
    slot: Weak<Slot>,
    instance: zero_core::component::ComponentInstanceId,
    owner: Weak<Shared>,
    binding: Weak<BoundAuthority>,
    driver: TaskJoinWitness,
}
impl CarrierAdoption {
    pub fn retained_driver(&self) -> Option<CarrierDriverRetained> {
        let owner = self.slot.owner.upgrade()?;
        let binding = owner.state().binding.clone()?;
        let driver = self.slot.state().driver.as_ref()?.1.clone();
        let proof = CarrierDriverRetained {
            slot: Arc::downgrade(&self.slot),
            instance: self.slot.instance,
            owner: Arc::downgrade(&owner),
            binding: Arc::downgrade(&binding),
            driver,
        };
        proof.actual()?;
        Some(proof)
    }
}
impl CarrierDriverRetained {
    pub(crate) fn instance(&self) -> zero_core::component::ComponentInstanceId {
        self.instance
    }
    pub(crate) fn names_original(&self, original: &CarrierAdoption) -> bool {
        self.slot.ptr_eq(&Arc::downgrade(&original.slot))
    }
    fn actual(&self) -> Option<(Arc<Slot>, Arc<Shared>)> {
        let slot = self.slot.upgrade()?;
        let owner = self.owner.upgrade()?;
        let binding = self.binding.upgrade()?;
        {
            let state = owner.state();
            if !state.live
                || state.stopped
                || !state.slots.iter().any(|actual| Arc::ptr_eq(actual, &slot))
                || !state
                    .binding
                    .as_ref()
                    .is_some_and(|actual| Arc::ptr_eq(actual, &binding))
            {
                return None;
            }
        }
        if self.driver.is_joined() || !binding.retains_driver(&self.driver) {
            return None;
        }
        {
            let state = slot.state();
            if state.closed
                || state.unknown.is_some()
                || !state
                    .users
                    .as_ref()
                    .is_some_and(super::users::Users::driver_custody_live)
                || !state
                    .driver
                    .as_ref()
                    .is_some_and(|(_, actual)| actual.same_registration(&self.driver))
                || (state.control.is_none() && state.packet.is_none() && state.loan.is_none())
            {
                return None;
            }
        }
        Some((slot, owner))
    }
    pub fn adoption(&self) -> Option<CarrierAdoption> {
        self.actual().map(|(slot, _)| CarrierAdoption { slot })
    }
    pub fn owner(&self) -> Option<CarrierOwnerRegistration> {
        self.actual()
            .map(|(_, owner)| CarrierOwnerRegistration::new(&owner))
    }
    pub fn driver(&self) -> &TaskJoinWitness {
        &self.driver
    }
    pub fn retains(&self, original: &CarrierAdoption) -> bool {
        self.actual()
            .is_some_and(|(slot, _)| Arc::ptr_eq(&slot, &original.slot))
    }
}
