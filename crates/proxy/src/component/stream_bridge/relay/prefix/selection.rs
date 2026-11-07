//! Service re-selection rebuilds only the same strictly shorter original path.
use super::*;
use zero_core::request::{ServiceOrigin, ServicePurpose, ServiceRequestFacts};
use zero_platform_tokio::contract::{
    context::RequestIdentity,
    network::{PendingCarrierObserver, UpstreamTransport},
};
impl PrefixOpener {
    pub(super) async fn select_shorter(
        &self,
        call: &CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> Result<UpstreamTransport, ChannelError> {
        let shorter = Some(self.context.upstream.binding().clone());
        match waiting {
            Some(waiting) => {
                self.context
                    .upstream
                    .select_background_carrier_observed(shorter, call.clone(), waiting)
                    .await
            }
            None => {
                self.context
                    .upstream
                    .select_background_carrier(shorter, call.clone())
                    .await
            }
        }
    }
    pub(super) fn reselect(
        &self,
        binding: Option<UpstreamBinding>,
        call: CarrierContext,
        waiting: Option<PendingCarrierObserver>,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        Box::pin(async move {
            let binding = binding.ok_or_else(|| {
                invalid(
                    Reason::StalePlan,
                    "prefix requires its exact selected binding",
                )
            })?;
            if binding != self.expected_binding
                || call.session.is_some()
                || call.cancel.is_cancelled()
                || binding.network_generation.0 != self.network.network_generation()
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "selected Service prefix changed",
                ));
            }
            let context = OpenContext {
                identity: self.service_identity.clone().unwrap_or_else(|| {
                    RequestIdentity::Service(
                        Arc::new(ServiceRequestFacts {
                            origin: ServiceOrigin::Runtime,
                            purpose: ServicePurpose::BackgroundControl,
                        })
                        .into(),
                    )
                }),
                deadline: call.deadline,
                cancel: call.cancel.clone(),
                observer: if self.context.session_identity()?.is_none() {
                    self.context.observer.clone()
                } else {
                    None
                },
                upstream: self.select_shorter(&call, waiting).await?,
            };
            let mut selected = PrefixOpener::for_service(
                self.role.clone(),
                context,
                binding.clone(),
                self.expected_endpoint.clone(),
                self.request_destination.clone(),
                self.network.clone(),
                self.observer.clone(),
            );
            selected.service_identity = self.service_identity.clone();
            selected.datagram_slot = self.datagram_slot.clone();
            selected.plan = self.plan.clone();
            selected.health = self.health.clone();
            Ok(UpstreamTransport::new(binding, Arc::new(selected)))
        })
    }
}
