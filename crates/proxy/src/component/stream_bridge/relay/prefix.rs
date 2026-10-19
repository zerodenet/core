use super::health::FirstHopHealth;
use crate::component::instance::invalid;
use crate::protocol_registry::UpstreamConnectServices;
use std::sync::Arc;
use zero_core::request::Destination;
use zero_platform_tokio::contract::{
    context::{with_deadline, OpenContext},
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding, UpstreamOpener},
    stream::{BoxStream, ConnectRequest, DynStreamOutbound},
    BoxFuture, ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(super) struct PrefixOpener {
    pub(super) role: Arc<dyn DynStreamOutbound>,
    pub(super) context: OpenContext,
    pub(super) expected_binding: UpstreamBinding,
    pub(super) expected_endpoint: Destination,
    pub(super) request_destination: Destination,
    pub(super) network: UpstreamConnectServices,
    pub(super) observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pub(super) health: Option<FirstHopHealth>,
}
impl PrefixOpener {
    fn interrupted(&self, carrier: &CarrierContext, reason: Reason) -> ChannelError {
        let mut error = ChannelError::new(Stage::Dial, reason, Impact::Session);
        error.component = self
            .expected_binding
            .prefix
            .last()
            .map(|role| role.component);
        error.session = carrier.session;
        // The prefix may already have sent a proxy request while opening this
        // carrier. Never claim a retryable empty root after cancellation.
        error.delivery = DeliveryState::Unknown;
        error
    }
}
impl UpstreamOpener for PrefixOpener {
    fn open_stream(
        &self,
        binding: &UpstreamBinding,
        destination: Destination,
        carrier: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxStream, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            if binding != self.expected_binding
                || self.network.network_generation() != binding.network_generation.0
                || destination != self.expected_endpoint
                || carrier.session != self.context.session_identity()?
            {
                let mut error = invalid(
                    Reason::StalePlan,
                    "selected carrier prefix, endpoint, request or generation changed",
                );
                error.component = self
                    .expected_binding
                    .prefix
                    .last()
                    .map(|role| role.component);
                error.session = self.context.identity.session();
                return Err(error);
            }
            let mut context = self.context.clone();
            context.deadline = match (context.deadline, carrier.deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            let deadline = context.deadline;
            // Keep the host's parent token. A protocol may add cancellation,
            // but cannot replace the parent's fence with an unrelated token.
            let attempt = tokio::select! {
                biased;
                _ = self.context.cancel.cancelled() => Err(self.interrupted(&carrier, Reason::Cancelled)),
                _ = carrier.cancel.cancelled() => Err(self.interrupted(&carrier, Reason::Cancelled)),
                result = with_deadline(deadline, self.role.connect(ConnectRequest { destination: self.request_destination.clone() }, context)) => result.unwrap_or_else(|source| Err(self.interrupted(&carrier, Reason::Timeout).with_source(source))),
            };
            if let Some(health) = &self.health {
                health.observe(&attempt);
            }
            let connected = attempt?;
            if self.network.network_generation() != binding.network_generation.0 {
                // The previous role may already have submitted a CONNECT.
                // Dropping the stale carrier cannot undo that submission.
                return Err(self.interrupted(&carrier, Reason::StalePlan).with_source(
                    std::io::Error::other("network changed during prefix establishment"),
                ));
            }
            Ok(match &self.observer {
                Some(observer) => Box::pin(zero_transport::observed::ObservedStream::new(
                    connected.io,
                    observer.clone(),
                )) as BoxStream,
                None => connected.io,
            })
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
                Reason::Internal,
                "Stream prefix has no datagram grant",
            ))
        })
    }
}

#[cfg(test)]
#[path = "../../tests/relay_prefix.rs"]
mod tests;
