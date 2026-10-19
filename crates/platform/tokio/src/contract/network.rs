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

/// Host-supplied execution of an already selected prefix. Implementations must
/// reject stale generations and preserve every hard dial constraint. No direct fallback.
pub trait UpstreamOpener: Send + Sync {
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
    pub fn binding(&self) -> &UpstreamBinding {
        &self.binding
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
