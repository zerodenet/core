//! Exact Datagram ticket, independent of Stream preparation permission.
use super::*;
impl CarrierOwnerBinding {
    /// Datagram-only ticket for this same original prepared Direct slot.
    /// A Stream-only declaration/ticket never acquires this permission.
    pub fn prepared_direct_datagram_service_issuer(
        &self,
        original_host_fence: &Arc<CancellationToken>,
        grant: &CarrierGrant,
        caller: TaskJoinWitness,
        endpoint: &Destination,
        upstream: &UpstreamBinding,
    ) -> Result<CarrierServiceIssuer, ChannelError> {
        if !self.supports_direct_datagram() {
            return Err(failure(grant.instance(), Reason::Unsupported));
        }
        let mut issuer = self.prepared_direct_service_issuer(
            original_host_fence,
            grant,
            caller,
            endpoint,
            upstream,
        )?;
        issuer.datagram = true;
        Ok(issuer)
    }
}
impl CarrierServiceGrant {
    pub(in crate::controlled_carrier) fn permits_direct_datagram_authority(
        &self,
        authority: &Arc<super::super::binding::BoundAuthority>,
    ) -> bool {
        self.direct
            && self.datagram
            && self.binding.supports_direct_datagram()
            && self.binding.matches_authority(authority)
            && self
                .binding
                .matches_direct_service(&self.endpoint, &self.upstream)
    }
}
