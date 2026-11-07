//! Same actual original, host binding and registered noncached job custody.
use super::*;
#[derive(Clone)]
pub struct CarrierPreparationJobRetained {
    pub(super) slot: Weak<Slot>,
    pub(super) job: Arc<Job>,
}
impl CarrierPreparationJobRetained {
    fn actual(&self) -> Option<(Arc<Slot>, Arc<crate::controlled_carrier::owner::Shared>)> {
        let slot = self.slot.upgrade()?;
        let owner = slot.owner.upgrade()?;
        let bound = {
            let state = owner.state();
            if !state.slots.iter().any(|actual| Arc::ptr_eq(actual, &slot)) {
                return None;
            }
            state.binding.clone()?
        };
        let driver = self.job.driver.get()?;
        if !self.job.sealed.load(Ordering::Acquire)
            || driver.is_joined()
            || !bound.observes_preparation_job(driver)
            || !driver.scope_identity().same_scope(&self.job.scope)
        {
            return None;
        }
        {
            let state = slot.state();
            if state.closed
                || state.unknown.is_some()
                || !state.view_live
                || !state.users.as_ref().is_some_and(|users| {
                    users.live()
                        && users.acquired()
                        && users
                            .preparation
                            .as_ref()
                            .and_then(|p| p.job.get())
                            .is_some_and(|actual| Arc::ptr_eq(actual, &self.job))
                })
                || !state
                    .driver
                    .as_ref()
                    .is_some_and(|(_, actual)| actual.same_registration(driver))
                || (state.control.is_none() && state.loan.is_none())
            {
                return None;
            }
        }
        Some((slot, owner))
    }
    pub fn retains(&self, original: &crate::controlled_carrier::CarrierAdoption) -> bool {
        self.actual()
            .is_some_and(|(slot, _)| Arc::ptr_eq(&slot, &original.slot))
    }
    pub fn owner(&self) -> Option<crate::controlled_carrier::CarrierOwnerRegistration> {
        self.actual()
            .map(|(_, owner)| crate::controlled_carrier::CarrierOwnerRegistration::new(&owner))
    }
    pub fn driver(&self) -> &TaskJoinWitness {
        self.job
            .driver
            .get()
            .expect("actual job driver installed before sealed proof")
    }
}
