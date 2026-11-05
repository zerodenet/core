//! Actual Service Node DNS preserves the one invocation's retry authority.
use super::*;
impl NodeOpener {
    pub(super) fn prepare_service_stream(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<PreparedCarrier, ChannelError>> {
        Box::pin(async move {
            let invocation = self.service_call.as_ref().ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "Service DNS requires its actual invocation",
                )
            })?;
            if call.session.is_some()
                || invocation.session.is_some()
                || endpoint != self.endpoint
                || !binding.prefix.is_empty()
                || binding.network_generation.0 != self.generation
                || self.services.network_generation() != self.generation
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "Service Node DNS binding changed",
                ));
            }
            let mut cached = self.service_candidates.lock().await;
            if cached.is_none() {
                let resolver =
                    zero_platform_tokio::controlled_carrier::ResolvingCarrierOpener::new(
                        grant.instance(),
                        binding.clone(),
                        endpoint.clone(),
                        self.services.native_node_dns(),
                        Arc::new(self.services.egress_control()),
                        self.services.observer(),
                    )?;
                *cached = Some((
                    resolver
                        .resolve_controlled_carrier(&binding, endpoint, invocation.clone())
                        .await?,
                    0,
                ));
            }
            let (candidates, next) = cached.as_mut().unwrap();
            if *next == candidates.len() {
                return Err(invalid(Reason::Io, "Service Node DNS candidates exhausted"));
            }
            let prepared = candidates
                .prepare_stream(*next, grant)?
                .with_endpoint_observer(Arc::new(ConnectedEvidence(self.evidence.clone())));
            *next += 1;
            Ok(prepared)
        })
    }
}
