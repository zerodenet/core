use std::sync::Arc;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use zero_core::{
    component::{NetworkGeneration, RoleInstanceId, SessionId},
    request::Destination,
};
use zero_traits::DialPolicy;

use super::{
    datagram::{BoxDatagramSession, DatagramRequest},
    stream::BoxStream,
    BoxFuture, ChannelError,
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

/// Host-supplied execution of an already selected prefix. Implementations must
/// reject stale generations and preserve every hard dial constraint. No direct fallback.
pub trait UpstreamOpener: Send + Sync {
    fn borrow_datagram(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::DatagramLoan, ChannelError>> {
        Box::pin(async {
            let mut error = ChannelError::new(
                super::Stage::Prepare,
                super::Reason::Unsupported,
                super::Impact::Component,
            );
            error.delivery = super::DeliveryState::NotSubmitted;
            Err(error)
        })
    }
    /// Observation only: retain no I/O authority and issue no close receipt.
    fn observe_carrier_origin(&self, _: &crate::controlled_carrier::CarrierOrigin) -> bool {
        false
    }
    /// A persistent host provider selects a fresh actual binding, without
    /// retaining a business opener, registry or historical configuration.
    fn select_background_carrier(
        &self,
        _: Option<UpstreamBinding>,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<UpstreamTransport, ChannelError>> {
        let mut error = ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        error.delivery = super::DeliveryState::NotSubmitted;
        Box::pin(async move { Err(error) })
    }
    /// Opt-in logical borrowing from the selected real donor. An unowned
    /// BoxStream is never promoted to a controlled carrier after opening it.
    fn borrow_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::loan::StreamLoan, ChannelError>> {
        let mut error = ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        error.delivery = super::DeliveryState::NotSubmitted;
        Box::pin(async move { Err(error) })
    }
    fn carrier_dns_generation(&self) -> Option<super::dns::DnsGeneration> {
        None
    }
    /// Host-owned node resolution only. The result retains the original domain
    /// and full binding; it can prepare candidates only against that declaration.
    fn resolve_controlled_carrier(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierCandidates, ChannelError>> {
        let mut error = ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        error.delivery = super::DeliveryState::NotSubmitted;
        Box::pin(async move { Err(error) })
    }
    /// Synchronous original acquisition; the same persistent owner is installed
    /// before the returned packet view can be polled or moved into a driver.
    fn prepare_controlled_datagram(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::CarrierDatagram, ChannelError> {
        let mut error = ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        error.component = Some(grant.instance().component);
        error.delivery = super::DeliveryState::NotSubmitted;
        Err(error)
    }
    /// Selection hint only. It issues no grant; the controlled open must still
    /// validate the original request, owner, caller and selected generation.
    fn supports_controlled_stream(&self) -> bool {
        false
    }

    /// Multi-candidate resolution/acquisition is opt-in. Reject before legacy
    /// execution or native I/O; an unsupported opener cannot silently fall back.
    fn open_controlled_stream(
        &self,
        _: &UpstreamBinding,
        request: ControlledStreamRequest,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let mut error = ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        error.component = Some(request.grants.instance().component);
        error.session = request.context.session;
        error.delivery = super::DeliveryState::NotSubmitted;
        Box::pin(async move {
            Err(error.with_source(std::io::Error::other(
                "controlled multi-candidate stream capability was not granted",
            )))
        })
    }

    /// Acquire controlled I/O under the caller's original reservation, whether
    /// native or borrowed. The default consumes synchronous native preparation;
    /// async DNS and logical loans implement this same acquisition boundary.
    fn acquire_controlled_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> BoxFuture<'_, Result<crate::controlled_carrier::CarrierIo, ChannelError>> {
        let prepared = self.prepare_controlled_stream(binding, destination, context.clone(), grant);
        Box::pin(async move {
            let prepared = prepared?;
            Ok(prepared
                .open_with_deadline(context.deadline)
                .await?
                .into_parts()
                .0)
        })
    }
    /// Controlled capability is opt-in. Reject before any legacy opener or I/O.
    fn prepare_controlled_stream(
        &self,
        _: &UpstreamBinding,
        _: Destination,
        _: CarrierContext,
        grant: crate::controlled_carrier::CarrierGrant,
    ) -> Result<crate::controlled_carrier::PreparedCarrier, ChannelError> {
        let mut error = super::ChannelError::new(
            super::Stage::Prepare,
            super::Reason::Unsupported,
            super::Impact::Component,
        );
        error.component = Some(grant.instance().component);
        error.delivery = super::DeliveryState::NotSubmitted;
        Err(error.with_source(std::io::Error::other(
            "controlled carrier capability was not granted",
        )))
    }
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>>;
    fn open_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>>;
}

#[derive(Clone)]
pub struct UpstreamTransport {
    binding: Arc<UpstreamBinding>,
    opener: Arc<dyn UpstreamOpener>,
}

impl UpstreamTransport {
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
