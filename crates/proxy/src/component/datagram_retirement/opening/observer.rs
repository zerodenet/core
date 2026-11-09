//! Callback bound to the actual finite association and exact upstream binding.
use super::*;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    stream::BoxStream,
};
impl UpstreamOpener for Observer {
    fn protocol_original_observer(&self) -> Option<PendingCarrierObserver> {
        self.pending.upgrade()?.protocols.as_ref()?;
        self.anchor.get().cloned()
    }
    fn retain_protocol_original(
        &self,
        binding: &UpstreamBinding,
        original: zero_platform_tokio::controlled_carrier::ProtocolOriginal,
    ) -> Result<(), ChannelError> {
        let fail = || admission_error(Reason::StalePlan);
        let anchor = self.anchor.get().ok_or_else(fail)?;
        if anchor.binding() != binding || !original.matches_observer(anchor) {
            return Err(fail());
        }
        let pending = self
            .pending
            .upgrade()
            .ok_or_else(|| admission_error(Reason::Revoked))?;
        let caller = pending.caller.as_ref().ok_or_else(fail)?;
        if pending.stop.is_cancelled()
            || caller.is_joined()
            || !caller
                .native_task_id()
                .is_some_and(|id| Some(id) == tokio::task::try_id())
        {
            return Err(fail());
        }
        let plan = pending.plan.upgrade().ok_or_else(fail)?;
        let source = pending.source.upgrade().ok_or_else(fail)?;
        let services = source
            .runtime_services()
            .map_err(|source| fail().with_source(source))?;
        if services.upstream().network_generation() != binding.network_generation.0
            || original.binding().network_generation != binding.network_generation
            || !pending.roles.iter().enumerate().any(|(index, role)| {
                original.binding().prefix == pending.roles[..index]
                    && plan.validate_protocol_original(*role, &original).is_ok()
            })
        {
            return Err(fail());
        }
        pending
            .protocols
            .as_ref()
            .ok_or_else(fail)?
            .retain_for_caller(original, caller.clone())
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if self
            .anchor
            .get()
            .is_none_or(|anchor| binding != anchor.binding())
        {
            return None;
        }
        Some(self.pending.upgrade()?.limit)
    }

    fn has_pending_carrier_observer(&self) -> bool {
        self.pending
            .upgrade()
            .is_some_and(|pending| pending.limit != 0)
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if self
            .anchor
            .get()
            .is_none_or(|anchor| binding != anchor.binding())
        {
            return Err(admission_error(Reason::StalePlan));
        }
        self.pending
            .upgrade()
            .ok_or_else(|| admission_error(Reason::Revoked))?
            .retain(adoption)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { Err(admission_error(Reason::Unsupported)) })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { Err(admission_error(Reason::Unsupported)) })
    }
}
