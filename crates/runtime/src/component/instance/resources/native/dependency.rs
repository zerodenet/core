//! Existing exact direct graph edges, not a new dependency registry or pool.
use super::*;
use crate::component::instance::ComponentSlot;
use zero_core::component::ComponentInstanceId;
use zero_platform_tokio::{
    contract::network::UpstreamBinding,
    controlled_carrier::{loan::CarrierDependencyLoanIssuer, CarrierGrant},
};
impl ComponentSlot {
    pub(in crate::component) fn bind_dependency_slots(&self, slots: Arc<[std::sync::Weak<Self>]>) {
        if let Some(actual) = self.dependency_slots.get() {
            assert!(
                actual.len() == slots.len()
                    && actual.iter().zip(slots.iter()).all(|(a, b)| a.ptr_eq(b)),
                "reused slot keeps the same actual dependencies"
            );
        } else {
            assert!(
                self.dependency_slots.set(slots).is_ok(),
                "one cold host dependency binding"
            );
        }
    }
}
impl HostResources {
    pub(in crate::component) fn bind_dependency_slots(
        &self,
        slots: Arc<[std::sync::Weak<ComponentSlot>]>,
    ) {
        assert!(
            self.dependency_slots.set(slots).is_ok(),
            "one actual host dependency binding"
        );
    }
    pub(super) fn registered_dependency_stream_issuer(
        &self,
        dependency: ComponentInstanceId,
        endpoint: &Destination,
        binding: &UpstreamBinding,
        grant: &CarrierGrant,
    ) -> Result<CarrierDependencyLoanIssuer, ChannelError> {
        if self.cancel.is_cancelled()
            || !binding.prefix.is_empty()
            || self.instance != Some(grant.instance())
        {
            return Err(self.denied());
        }
        let donor = self
            .dependency_slots
            .get()
            .into_iter()
            .flat_map(|slots| slots.iter())
            .filter_map(std::sync::Weak::upgrade)
            .find(|slot| slot.instance_id() == dependency)
            .ok_or_else(|| self.denied())?;
        donor.check_dependency_loan_start()?;
        let owners: Vec<_> = donor
            .recipe
            .spec()
            .resources
            .iter()
            .enumerate()
            .filter_map(|(ordinal, _)| donor.original_carrier_owner(ordinal))
            .collect();
        let [donor_owner] = owners.as_slice() else {
            return Err(self.denied());
        };
        let bound = self.carriers.registered_dependency_receiver(grant)?;
        // No catalog/record/Scope mutex surrounds any provider getter or drop.
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed || state.start_deadline.is_some() {
            return Err(self.denied());
        }
        drop(state);
        bound.dependency_stream_loan_issuer(
            self.carriers.host_fence()?,
            grant,
            donor_owner,
            endpoint,
            binding,
        )
    }
}
