use super::*;
use crate::contract::network::UpstreamOpener;
pub(super) struct Observer(pub(super) RecordWeak);
impl Observer {
    fn actual_opener(
        &self,
        binding: &UpstreamBinding,
        call: &CarrierContext,
    ) -> Result<crate::contract::network::UpstreamOpenerLease, ChannelError> {
        let record = self.record(binding)?;
        self.packet_caller(
            binding,
            &RequestIdentity::Service(record.identity.clone().into()),
            call,
        )?;
        // During prefix construction only observation/custody is available.
        // IO delegation starts after the actual selected opener is installed.
        record
            .upstream
            .get()
            .ok_or_else(|| failure(Reason::Revoked))?
            .upgrade_exact(binding)
    }
    fn record(&self, binding: &UpstreamBinding) -> Result<RecordRef, ChannelError> {
        let record = self.0.upgrade().ok_or_else(|| failure(Reason::Revoked))?;
        if record.binding != *binding {
            return Err(failure(Reason::StalePlan));
        }
        Ok(record)
    }
}
impl UpstreamOpener for Observer {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: zero_core::request::Destination,
        call: CarrierContext,
    ) -> crate::contract::BoxFuture<'_, Result<crate::contract::stream::BoxStream, ChannelError>>
    {
        let actual = self.actual_opener(binding, &call);
        let binding = binding.clone();
        Box::pin(async move { actual?.open_stream(&binding, destination, call).await })
    }
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: crate::contract::datagram::DatagramRequest,
        call: CarrierContext,
    ) -> crate::contract::BoxFuture<
        '_,
        Result<crate::contract::datagram::BoxDatagramSession, ChannelError>,
    > {
        let actual = self.actual_opener(binding, &call);
        let binding = binding.clone();
        Box::pin(async move { actual?.open_datagram(&binding, request, call).await })
    }

    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        Some(self.record(binding).ok()?.max_pending)
    }
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<TaskJoinWitness, ChannelError> {
        let record = self.record(binding)?;
        let same = matches!(identity, RequestIdentity::Service(actual) if actual.same_owner(&record.identity));
        if !same
            || record.call.cancel.is_cancelled()
            || call.cancel.is_cancelled()
            || record.caller.is_joined()
            || !record.caller.scope_identity().resource_open()
            || call.session.is_some()
            || call.deadline != record.call.deadline
            || call.deadline.is_some_and(|end| Instant::now() >= end)
        {
            return Err(failure(Reason::Revoked));
        }
        Ok(record.caller.clone())
    }
    fn packet_driver_custody(
        &self,
        binding: &UpstreamBinding,
        identity: &RequestIdentity,
        call: &CarrierContext,
    ) -> Result<Option<super::PacketDriverCallRetained>, ChannelError> {
        self.packet_caller(binding, identity, call)?;
        let record = self.record(binding)?;
        Ok(Some(super::attempt::PacketDriverPendingCustody::new(
            &record,
        )))
    }
    fn validate_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: &CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let record = self.record(binding)?;
        if !record
            .allowed
            .iter()
            .any(|owner| owner.matches_adoption(adoption))
        {
            return Err(failure(Reason::StalePlan));
        }
        Ok(())
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.validate_pending_carrier(binding, &adoption)?;
        let record = self.record(binding)?;
        let mut pending = record
            .pending
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if record.call.cancel.is_cancelled() {
            return Err(failure(Reason::Revoked));
        }
        if pending.iter().any(|actual| actual.same_slot(&adoption)) {
            return Ok(());
        }
        if pending.len() >= record.max_pending {
            return Err(failure(Reason::Capacity));
        }
        pending.push(super::original::PendingOriginal::new(
            adoption.clone(),
            &record,
        ));
        drop(pending);
        // The actual owner has strong custody before marking this exact view.
        // No native attempt or public caller can issue this sealed transfer.
        let custody = super::attempt::PacketDriverPendingCustody::new(&record);
        adoption.transfer_packet_driver_custody(custody)
    }
}
