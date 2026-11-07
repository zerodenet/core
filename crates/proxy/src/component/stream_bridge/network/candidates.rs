//! The actual background owner supplies only candidate provenance metadata.
use super::*;
impl NodeOpener {
    pub(in crate::component) fn with_carrier_owner(
        mut self,
        owner: zero_core::component::ComponentInstanceId,
    ) -> Self {
        self.carrier_owner = Some(owner);
        self
    }
    pub(super) fn resolve_background_candidates(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
    ) -> BoxFuture<
        '_,
        Result<zero_platform_tokio::controlled_carrier::CarrierCandidates, ChannelError>,
    > {
        Box::pin(async move {
            let owner = self.carrier_owner.ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "node has no actual background candidate owner",
                )
            })?;
            if endpoint != self.endpoint
                || !binding.prefix.is_empty()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
                || call.session.is_some()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "background Node candidate binding changed",
                ));
            }
            let resolver = zero_platform_tokio::controlled_carrier::ResolvingCarrierOpener::new(
                owner,
                binding.clone(),
                endpoint.clone(),
                self.services.native_node_dns(),
                Arc::new(self.services.egress_control()),
                self.services.observer(),
            )?;
            // This returns metadata from the same real Node query source.
            // Every actual candidate preparation still consumes and validates
            // its original host-issued grant; this field grants no native IO.
            resolver
                .resolve_controlled_carrier(&binding, endpoint, call)
                .await
        })
    }
}
