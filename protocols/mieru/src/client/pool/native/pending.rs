//! Only the actual pending record and a weak waiting cleanup callback.
use crate::client::pool::managed::{CarrierLease, PendingCarriers};
use std::sync::Arc;
use zero_platform_tokio::{
    contract::{
        datagram::{BoxDatagramSession, DatagramRequest},
        network::{
            CarrierContext, PendingCarrierObserver, UpstreamBinding, UpstreamOpener,
            UpstreamTransport,
        },
        stream::BoxStream,
        BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    controlled_carrier::CarrierAdoption,
};

pub(super) fn transport(
    lease: &CarrierLease,
    binding: UpstreamBinding,
    waiting: PendingCarrierObserver,
) -> UpstreamTransport {
    UpstreamTransport::new(
        binding.clone(),
        Arc::new(Pending {
            originals: lease.pending_carriers(),
            binding,
            waiting: Some(waiting),
            caller: None,
            provider: None,
        }),
    )
}
struct Pending {
    originals: PendingCarriers,
    binding: UpstreamBinding,
    waiting: Option<PendingCarrierObserver>,
    caller: Option<super::setup::Caller>,
    provider: Option<UpstreamTransport>,
}
pub(super) fn for_setup(
    originals: PendingCarriers,
    binding: UpstreamBinding,
    waiting: Option<PendingCarrierObserver>,
    caller: super::setup::Caller,
    provider: UpstreamTransport,
) -> UpstreamTransport {
    UpstreamTransport::new(
        binding.clone(),
        Arc::new(Pending {
            originals,
            binding,
            waiting,
            caller: Some(caller),
            provider: Some(provider),
        }),
    )
}
fn denied(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Component);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
impl UpstreamOpener for Pending {
    fn packet_caller(
        &self,
        binding: &UpstreamBinding,
        identity: &zero_platform_tokio::contract::context::RequestIdentity,
        call: &CarrierContext,
    ) -> Result<zero_platform_tokio::contract::scope::TaskJoinWitness, ChannelError> {
        if binding != &self.binding {
            return Err(denied(Reason::StalePlan));
        }
        self.caller
            .as_ref()
            .filter(|caller| &caller.binding == binding)
            .ok_or_else(|| denied(Reason::StalePlan))?
            .check(identity, call)
    }
    fn has_pending_carrier_observer(&self) -> bool {
        true
    }
    fn pending_carrier_capacity(&self, binding: &UpstreamBinding) -> Option<usize> {
        (binding == &self.binding)
            .then(|| self.originals.capacity())
            .flatten()
    }
    fn retain_pending_carrier(
        &self,
        binding: &UpstreamBinding,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        if binding != &self.binding {
            return Err(denied(Reason::StalePlan));
        }
        if let Some(provider) = &self.provider {
            provider.validate_pending_carrier(binding, &adoption)?;
            self.originals.bind_cleanup_provider(provider, binding)?;
        }
        // The actual caller must accept before I/O. Holding a record alone
        // does not cause cleanup when its connect future is dropped.
        if let Some(waiting) = &self.waiting {
            waiting.retain(adoption.clone())?;
        }
        self.originals.retain_pending(adoption)
    }
    fn open_stream(
        &self,
        _: &UpstreamBinding,
        _: zero_core::request::Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        Box::pin(async { Err(denied(Reason::Unsupported)) })
    }
    fn open_datagram(
        &self,
        _: &UpstreamBinding,
        _: DatagramRequest,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        Box::pin(async { Err(denied(Reason::Unsupported)) })
    }
}
