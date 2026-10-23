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
