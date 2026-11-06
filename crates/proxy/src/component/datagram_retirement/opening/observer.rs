//! Callback bound to the actual finite association and exact upstream binding.
use super::*;
use zero_platform_tokio::contract::{
    datagram::{BoxDatagramSession, DatagramRequest},
    stream::BoxStream,
};
impl UpstreamOpener for Observer {
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        if binding != &self.binding {
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
        if binding != &self.binding {
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
