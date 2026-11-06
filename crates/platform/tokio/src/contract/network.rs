use std::sync::Arc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zero_core::{
    component::{NetworkGeneration, RoleInstanceId, SessionId},
    request::Destination,
};
use zero_traits::DialPolicy;

use super::{
    BoxFuture, ChannelError,
    datagram::{BoxDatagramSession, DatagramRequest},
    stream::BoxStream,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct UpstreamBinding {
    pub prefix: Vec<RoleInstanceId>,
    pub dial: DialPolicy,
    pub network_generation: NetworkGeneration,
}

#[derive(Clone)]
pub struct CarrierContext {
    /// None for an explicitly declared background control carrier.
    pub session: Option<SessionId>,
    /// An additional absolute caller bound. Native operation timers still apply.
    pub deadline: Option<Instant>,
    pub cancel: CancellationToken,
}

/// A finite physical owner supplied by the actual selected role. Capacity and
/// weak task registration do not by themselves authorize a business session;
/// the host opener must validate its sealed demand for every real candidate.
pub struct ControlledStreamRequest {
    pub destination: Destination,
    pub context: CarrierContext,
    pub grants: crate::controlled_carrier::CarrierGrants,
    pub tasks: super::scope::WeakTaskRegistrar,
    /// Inline retirement budget only; never a replacement handshake deadline.
    pub retirement: std::time::Duration,
}

mod opener;
pub use opener::UpstreamOpener;

#[derive(Clone)]
pub struct UpstreamTransport {
    binding: Arc<UpstreamBinding>,
    opener: Arc<dyn UpstreamOpener>,
}

/// Weak reference to the actual waiting owner's existing cleanup ledger.
/// It cannot authorize native I/O and does not retain a historical caller.
#[derive(Clone)]
pub struct PendingCarrierObserver {
    binding: Arc<UpstreamBinding>,
    opener: std::sync::Weak<dyn UpstreamOpener>,
}
impl PendingCarrierObserver {
    pub fn capacity(&self) -> Option<usize> {
        self.opener
            .upgrade()?
            .pending_carrier_capacity(&self.binding)
    }

    pub fn retain(
        &self,
        adoption: crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        let opener = self.opener.upgrade().ok_or_else(|| {
            let mut error = ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Revoked,
                super::Impact::Component,
            );
            error.delivery = super::DeliveryState::NotSubmitted;
            error
        })?;
        opener.retain_pending_carrier(&self.binding, adoption)
    }
}

impl UpstreamTransport {
    pub fn pending_carrier_capacity(&self) -> Option<usize> {
        self.opener.pending_carrier_capacity(&self.binding)
    }
    pub fn resolve_packet_target(
        &self,
        destination: Destination,
        identity: crate::contract::context::RequestIdentity,
        call: CarrierContext,
    ) -> BoxFuture<'_, Result<Vec<std::net::SocketAddr>, ChannelError>> {
        self.opener
            .resolve_packet_target(&self.binding, destination, identity, call)
    }

    pub fn resolve_related_datagram(
        &self,
        parent: crate::controlled_carrier::CarrierAdoption,
        peer: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierCandidates, ChannelError>> {
        self.opener
            .resolve_related_datagram(&self.binding, parent, peer, context)
    }
    pub fn has_tracked_business_caller(&self) -> bool {
        self.opener.has_tracked_business_caller()
    }
    pub fn observed_pending_carrier(&self) -> Option<PendingCarrierObserver> {
        self.opener
            .has_pending_carrier_observer()
            .then(|| self.pending_carrier_observer())
    }
    pub fn has_pending_carrier_observer(&self) -> bool {
        self.opener.has_pending_carrier_observer()
    }
    pub fn pending_carrier_observer(&self) -> PendingCarrierObserver {
        PendingCarrierObserver {
            binding: self.binding.clone(),
            opener: Arc::downgrade(&self.opener),
        }
    }
    pub fn select_background_carrier_observed(
        &self,
        binding: Option<UpstreamBinding>,
        context: CarrierContext,
        waiting: PendingCarrierObserver,
    ) -> BoxFuture<'_, Result<Self, ChannelError>> {
        self.opener
            .select_background_carrier_observed(binding, context, waiting)
    }
    pub fn retain_pending_carrier(
        &self,
        adoption: crate::controlled_carrier::CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.opener.retain_pending_carrier(&self.binding, adoption)
    }
    pub fn borrow_datagram(
        &self,
        endpoint: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::DatagramLoan, ChannelError>> {
        self.opener
            .borrow_datagram(&self.binding, endpoint, context)
    }
    pub fn observe_carrier_origin(
        &self,
        origin: &crate::controlled_carrier::CarrierOrigin,
    ) -> bool {
        self.opener.observe_carrier_origin(origin)
    }
    pub fn select_background_carrier(
        &self,
        binding: Option<UpstreamBinding>,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<Self, ChannelError>> {
        self.opener.select_background_carrier(binding, context)
    }
    pub fn borrow_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::StreamLoan, ChannelError>> {
        self.opener
            .borrow_stream(&self.binding, destination, context)
    }
    pub fn carrier_dns_generation(&self) -> Option<super::dns::DnsGeneration> {
        self.opener.carrier_dns_generation()
    }
    pub fn resolve_controlled_carrier(
        &self,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierCandidates, ChannelError>> {
        self.opener
            .resolve_controlled_carrier(&self.binding, destination, context)
    }
    pub fn prepare_controlled_datagram(
        &self,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::CarrierDatagram, ChannelError> {
        self.opener
            .prepare_controlled_datagram(&self.binding, destination, context, grant)
    }
    pub fn new(binding: UpstreamBinding, opener: Arc<dyn UpstreamOpener>) -> Self {
        Self {
            binding: Arc::new(binding),
            opener,
        }
    }
    pub fn supports_controlled_stream(&self) -> bool {
        self.opener.supports_controlled_stream()
    }
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
    }
    pub fn prepare_controlled_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::PreparedCarrier, ChannelError> {
        self.opener
            .prepare_controlled_stream(&self.binding, destination, context, grant)
    }
    pub fn acquire_controlled_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierIo, ChannelError>> {
        self.opener
            .acquire_controlled_stream(&self.binding, destination, context, grant)
    }

    pub fn open_controlled_stream(
        &self,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.opener.open_controlled_stream(&self.binding, request)
    }
    pub fn open_stream(
        &self,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        self.opener.open_stream(&self.binding, destination, context)
    }
    pub fn open_datagram(
        &self,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        self.opener.open_datagram(&self.binding, request, context)
    }
}
