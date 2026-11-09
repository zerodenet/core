//! Same admitted Business ticket, with its actual pre-delivery setup caller.
use super::*;
use zero_platform_tokio::{
    contract::{
        context::RequestIdentity,
        network::{PendingCarrierObserver, UpstreamTransport},
        scope::TaskJoinWitness,
    },
    controlled_carrier::{CarrierAdoption, CarrierPreparation},
};
struct PreparingCaller {
    preparation: CarrierPreparation,
    original: crate::component::business_carrier::BusinessCarrierRequest,
    identity: RequestIdentity,
    original_call: CarrierContext,
    service_call: CarrierContext,
    binding: UpstreamBinding,
    waiting: Option<PendingCarrierObserver>,
}
impl PrefixOpener {
    pub(super) fn preparation_transport(
        &self,
        preparation: CarrierPreparation,
        original: crate::component::business_carrier::BusinessCarrierRequest,
        original_call: CarrierContext,
        service_call: CarrierContext,
    ) -> Result<UpstreamTransport, ChannelError> {
        let binding = self.context.upstream.binding().clone();
        let admitted = original.packet_caller(&self.context.identity, &original_call)?;
        let waiting = preparation
            .pending_observer(&admitted)?
            .or_else(|| self.context.upstream.observed_pending_carrier());
        let pending = PreparingCaller {
            preparation,
            original,
            identity: self.context.identity.clone(),
            original_call,
            service_call,
            binding: binding.clone(),
            waiting,
        };
        pending.caller()?;
        Ok(UpstreamTransport::new(binding, Arc::new(pending)))
    }
}
impl PreparingCaller {
    fn caller(&self) -> Result<TaskJoinWitness, ChannelError> {
        let admitted = self
            .original
            .packet_caller(&self.identity, &self.original_call)?;
        self.preparation.preparing_caller(&admitted)
    }
}
impl UpstreamOpener for PreparingCaller {
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        if binding != &self.binding
            || call.session.is_some()
            || call.deadline != self.service_call.deadline
            || call.cancel.is_cancelled()
            || self.service_call.cancel.is_cancelled()
            || !matches!(identity, RequestIdentity::Service(facts) if facts.origin == zero_core::request::ServiceOrigin::Runtime
                && facts.purpose == zero_core::request::ServicePurpose::BackgroundControl)
        {
            return Err(invalid(
                Reason::StalePlan,
                "actual staged Business donor call changed",
            ));
        }
        self.caller()
    }
    fn protocol_original_observer(
        &self,
    ) -> Option<zero_platform_tokio::contract::network::PendingCarrierObserver> {
        self.waiting
            .as_ref()
            .and_then(|waiting| waiting.protocol_original_observer())
    }
    fn has_pending_carrier_observer(&self) -> bool {
        self.waiting.is_some()
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding != &self.binding {
            return None;
        }
        self.caller().ok()?;
        self.waiting.as_ref()?.capacity()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != &self.binding {
            return Err(invalid(
                Reason::StalePlan,
                "actual staged pending binding changed",
            ));
        }
        self.caller()?;
        self.waiting
            .as_ref()
            .ok_or_else(|| {
                invalid(
                    Reason::Unsupported,
                    "actual staged caller has no foreign pending ledger",
                )
            })?
            .retain(adoption)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "setup observation gives no Stream IO",
            ))
        })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async {
            Err(invalid(
                Reason::Unsupported,
                "setup observation gives no Datagram IO",
            ))
        })
    }
}
