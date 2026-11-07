//! A single real selected borrow; ordinary scalar borrowing is unchanged.
use super::*;
/// Move-only target issued for one receiver Slot, never a native permission.
pub struct ServiceDatagramTarget {
    pub(super) issuer: ServiceDatagramTargetIssuer,
    pub(super) peer: SocketAddr,
    pub(super) resolver: Option<(Arc<dyn DnsService>, DnsGeneration)>,
}
/// The actual lower unique loan travels with the selection until the receiving
/// original takes strong custody. Dropping this value never proves closed.
pub struct ServiceDatagramTargetLoan {
    target: ServiceDatagramTarget,
    loan: super::super::DatagramLoan,
}
impl ServiceDatagramTarget {
    pub fn binding(&self) -> &UpstreamBinding {
        &self.issuer.binding
    }
    pub fn endpoint(&self) -> &Destination {
        &self.issuer.endpoint
    }
    pub fn peer(&self) -> SocketAddr {
        self.peer
    }
    pub fn identity(&self) -> &RequestIdentity {
        &self.issuer.identity
    }
    pub fn call(&self) -> &CarrierContext {
        &self.issuer.call
    }
    /// Core validates the actual selected provider before calling the original
    /// Datagram role. Numeric equality alone cannot construct this selection.
    pub fn validate_borrow(
        &self,
        binding: &UpstreamBinding,
        endpoint: &Destination,
        current_network: NetworkGeneration,
    ) -> Result<(), ChannelError> {
        self.issuer.check_selected()?;
        if binding != self.binding()
            || endpoint != self.endpoint()
            || current_network != binding.network_generation
            || self
                .resolver
                .as_ref()
                .is_some_and(|(source, generation)| source.generation() != *generation)
        {
            return Err(failure(
                self.issuer.bound.registration().instance(),
                Reason::StalePlan,
            ));
        }
        Ok(())
    }
    /// No validation or permission is minted here: the original receiver must
    /// strong-install this exact LoanOwner before it verifies the selection.
    pub fn with_loan(self, loan: super::super::DatagramLoan) -> ServiceDatagramTargetLoan {
        ServiceDatagramTargetLoan { target: self, loan }
    }
}
impl ServiceDatagramTargetLoan {
    pub fn adopt(
        self,
        grant: CarrierGrant,
    ) -> Result<super::super::BorrowedDatagram, ChannelError> {
        let Self { target, loan } = self;
        grant.adopt_service_target_loan(
            &target.issuer.binding,
            target.issuer.endpoint.clone(),
            &target.issuer.call,
            loan,
            |grant| {
                target.issuer.check_installed(grant)?;
                if target
                    .resolver
                    .as_ref()
                    .is_some_and(|(source, generation)| source.generation() != *generation)
                {
                    return Err(failure(grant.instance(), Reason::StalePlan));
                }
                Ok(literal(target.peer))
            },
        )
    }
}
