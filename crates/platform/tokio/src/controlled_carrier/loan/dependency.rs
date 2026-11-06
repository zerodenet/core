//! Exact direct resource dependency, issued by the existing host's private fence.
use super::super::{
    binding::CarrierOwnerBinding, slot::Slot, CarrierGrant, CarrierOrigin, CarrierOwnerRegistration,
};
use crate::contract::{
    network::UpstreamBinding, CancellationToken, ChannelError, DeliveryState, Reason, Stage,
};
use std::sync::{Arc, Weak};
use zero_core::request::Destination;

/// One original reservation and its actual registered donor. Neither public IDs
/// nor a cloned cancellation token can reconstruct the host's issuer authority.
/// It grants no new IO, Scope, driver or permission to close the donor.
pub struct CarrierDependencyLoanIssuer {
    receiver: Weak<Slot>,
    bound: CarrierOwnerBinding,
    donor: CarrierOwnerBinding,
    endpoint: Destination,
    binding: UpstreamBinding,
}
impl CarrierOwnerBinding {
    /// Host only: first verify the exact direct dependency in this same actual
    /// plan. The private original cancellation Arc is never in ResourceAccess.
    pub fn dependency_stream_loan_issuer(
        &self,
        host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
        donor: &CarrierOwnerRegistration,
        endpoint: &Destination,
        binding: &UpstreamBinding,
    ) -> Result<CarrierDependencyLoanIssuer, ChannelError> {
        let fail = || {
            super::super::error(
                grant.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
        };
        let donor = donor.bound().ok_or_else(fail)?;
        if !binding.prefix.is_empty()
            || !self.matches_host_cancel(host_fence)
            || !self.registration().matches_grant(grant)
            || !self.authority.available()
            || !donor.authority.available()
            || self.authority.pool_kind() != Some(false)
            || donor.authority.pool_kind() != Some(false)
            || self.registration().instance() == donor.registration().instance()
        {
            return Err(fail());
        }
        Ok(CarrierDependencyLoanIssuer {
            receiver: Arc::downgrade(&grant.slot),
            bound: self.clone(),
            donor,
            endpoint: endpoint.clone(),
            binding: binding.clone(),
        })
    }
}
impl CarrierDependencyLoanIssuer {
    pub(super) fn permits(
        &self,
        grant: &CarrierGrant,
        binding: &UpstreamBinding,
        endpoint: &Destination,
        original: &CarrierOrigin,
    ) -> bool {
        if !self.receiver.ptr_eq(&Arc::downgrade(&grant.slot))
            || binding != &self.binding
            || endpoint != &self.endpoint
            || !self.bound.authority.available()
            || !self.donor.authority.available()
            || !self.bound.registration().matches_grant(grant)
            || !original.owner().same_owner(self.donor.registration())
            || !original.driver().is_some_and(|driver| {
                self.donor
                    .scope_identity()
                    .same_scope(&driver.scope_identity())
                    && !driver.is_joined()
            })
        {
            return false;
        }
        // Recheck exact original authority allocations after the donor await.
        self.bound
            .registration()
            .bound()
            .is_some_and(|current| current.matches_authority(&self.bound.authority))
            && self
                .donor
                .registration()
                .bound()
                .is_some_and(|current| current.matches_authority(&self.donor.authority))
    }
}
