//! Same actual Service grant and current host; no bare stream promotion.
use super::*;
use zero_platform_tokio::controlled_carrier::{
    CarrierEndpointObserver, CarrierEndpoints, CarrierGrant, CarrierIo, ResolvingCarrierOpener,
};
struct Connected(Arc<Mutex<Option<Evidence>>>);
impl CarrierEndpointObserver for Connected {
    fn connected(&self, endpoints: &CarrierEndpoints) {
        if let Some(local) = endpoints.local() {
            *self.0.lock().unwrap() = Some(Evidence {
                local,
                peer: endpoints.peer(),
            });
        }
    }
}
impl RootOpener {
    pub(super) fn acquire_original(
        &self,
        binding: UpstreamBinding,
        endpoint: Destination,
        call: CarrierContext,
        grant: CarrierGrant,
    ) -> BoxFuture<'_, Result<CarrierIo, ChannelError>> {
        Box::pin(async move {
            if binding != self.binding
                || endpoint != self.endpoint
                || !binding.prefix.is_empty()
                || call.session.is_some()
                || self.services.network_generation() != binding.network_generation.0
            {
                return Err(super::super::failure(Reason::StalePlan));
            }
            let source = self
                .source
                .upgrade()
                .ok_or_else(|| super::super::failure(Reason::Revoked))?;
            let current = source
                .runtime_services()
                .map_err(|source| super::super::failure(Reason::Revoked).with_source(source))?
                .execution();
            let plan = current.components.as_ref().map_err(|source| {
                super::super::failure(Reason::Revoked)
                    .with_source(crate::component::instance::retained(source.clone()))
            })?;
            plan.validate_carrier_adoption(self.selected, &grant.adoption())?;
            let mut cached = self.candidates.lock().await;
            if cached.is_none() {
                let resolver = ResolvingCarrierOpener::new(
                    grant.instance(),
                    binding.clone(),
                    endpoint.clone(),
                    self.services.native_node_dns(),
                    Arc::new(self.services.egress_control()),
                    self.services.observer(),
                )?;
                // Candidate authority belongs to the original caller. A failed
                // record cancels only that record, not the next candidate.
                *cached = Some((
                    resolver
                        .resolve_controlled_carrier(&binding, endpoint, self.call.clone())
                        .await?,
                    0,
                ));
            }
            let (candidates, next) = cached.as_mut().unwrap();
            if *next == candidates.len() {
                return Err(super::super::failure(Reason::Io));
            }
            let prepared = candidates
                .prepare_stream(*next, grant)?
                .with_endpoint_observer(Arc::new(Connected(self.evidence.clone())));
            *next += 1;
            drop(cached);
            Ok(prepared
                .open_with_deadline(call.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
}
