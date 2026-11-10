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
    donor: Donor,
    endpoint: Destination,
    binding: UpstreamBinding,
}
enum Donor {
    Carrier(CarrierOwnerBinding),
    Accepted {
        original: crate::AcceptedTcpCarrierOrigin,
        receiver: crate::AcceptedTcpTaskReceiver,
    },
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
        if !self.matches_host_cancel(host_fence)
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
            donor: Donor::Carrier(donor),
            endpoint: endpoint.clone(),
            binding: binding.clone(),
        })
    }
}
impl CarrierOwnerBinding {
    /// Host-only sealing after the actual plan checked this direct Source
    /// dependency. No numeric Source ID or Scope identity can replace its
    /// original receiver or this binding's private Host fence.
    pub fn accepted_dependency_stream_loan_issuer(
        &self,
        host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
        original: &crate::AcceptedTcpCarrierOrigin,
        receiver: crate::AcceptedTcpTaskReceiver,
        root: &Arc<super::super::CarrierOriginalTotals>,
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
        if !self.matches_host_cancel(host_fence)
            || !self.registration().matches_grant(grant)
            || !self.authority.available()
            || self.authority.pool_kind() != Some(false)
            || self.registration().instance() == original.instance()
            || !original.matches_receiver(&receiver)
            || !original.belongs_to_root(root)
            || original.is_retired()
            || !original.scope_identity().resource_open()
        {
            return Err(fail());
        }
        Ok(CarrierDependencyLoanIssuer {
            receiver: Arc::downgrade(&grant.slot),
            bound: self.clone(),
            donor: Donor::Accepted {
                original: original.clone(),
                receiver,
            },
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
        if let Donor::Accepted {
            original: accepted,
            receiver,
        } = &self.donor
        {
            return self.receiver.ptr_eq(&Arc::downgrade(&grant.slot))
                && binding == &self.binding
                && endpoint == &self.endpoint
                && self.bound.authority.available()
                && self.bound.registration().matches_grant(grant)
                && !accepted.is_retired()
                && accepted.scope_identity().resource_open()
                && accepted.matches_receiver(receiver)
                && original
                    .accepted()
                    .is_some_and(|actual| actual.same_original(accepted))
                && self
                    .bound
                    .registration()
                    .bound()
                    .is_some_and(|current| current.matches_authority(&self.bound.authority));
        }
        let Donor::Carrier(donor) = &self.donor else {
            unreachable!()
        };
        if !self.receiver.ptr_eq(&Arc::downgrade(&grant.slot))
            || binding != &self.binding
            || endpoint != &self.endpoint
            || !self.bound.authority.available()
            || !donor.authority.available()
            || !self.bound.registration().matches_grant(grant)
            // The actual direct dependency must have acquired this exact
            // endpoint/prefix/dial/network, not merely named scalar roles.
            || original.selected_binding().is_none_or(|(selected_endpoint, selected_binding)|
                selected_endpoint != self.endpoint || selected_binding != self.binding)
            || !original.owner().same_owner(donor.registration())
            || !original.driver().is_some_and(|driver| {
                donor
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
            && donor
                .registration()
                .bound()
                .is_some_and(|current| current.matches_authority(&donor.authority))
    }
}
