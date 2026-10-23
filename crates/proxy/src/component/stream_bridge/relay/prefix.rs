mod datagram;
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

pub(crate) struct PrefixOpener {
    pub(super) role: Arc<dyn DynStreamOutbound>,
    pub(super) datagram_slot: Option<Arc<crate::component::instance::ComponentSlot>>,
    pub(super) context: OpenContext,
    pub(super) expected_binding: UpstreamBinding,
    pub(super) expected_endpoint: Destination,
    pub(super) request_destination: Destination,
    pub(super) network: UpstreamConnectServices,
    pub(super) observer: Option<Arc<dyn zero_traits::IoObserver>>,
    pub(super) health: Option<FirstHopHealth>,
}
impl PrefixOpener {
    pub(crate) fn for_service(
        role: Arc<dyn DynStreamOutbound>,
        context: OpenContext,
        expected_binding: UpstreamBinding,
        expected_endpoint: Destination,
        request_destination: Destination,
        network: UpstreamConnectServices,
        observer: Option<Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        Self {
            role,
            context,
            expected_binding,
            expected_endpoint,
            request_destination,
            network,
            observer,
            health: None,
            // Added by the parallel raw Datagram provider. DNS TCP services
            // receive no unrelated Datagram grant.
            datagram_slot: None,
        }
    }
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
        binding: &UpstreamBinding,
        request: DatagramRequest,
        carrier: CarrierContext,
    ) -> BoxFuture<'_, Result<BoxDatagramSession, ChannelError>> {
        let binding = binding.clone();
        Box::pin(async move {
            let shorter = self.context.upstream.binding();
            if binding != self.expected_binding
                || binding.prefix.len() != shorter.prefix.len() + 1
                || binding.prefix[..shorter.prefix.len()] != shorter.prefix[..]
                || self.network.network_generation() != binding.network_generation.0
                || carrier.session != self.context.session_identity()?
            {
                return Err(self.interrupted(&carrier, Reason::StalePlan));
            }
            // A carrier open is for the concrete relay selected by the next
            // protocol, never permission to turn a Multiple session into root UDP.
            if !matches!(
                &request.targets,
                zero_core::request::DatagramTargets::Fixed(_)
            ) {
                return Err(self.interrupted(&carrier, Reason::Unsupported));
            }
            let slot = self
                .datagram_slot
                .as_ref()
                .ok_or_else(|| self.interrupted(&carrier, Reason::Unsupported))?;
            if binding.prefix.last() != Some(&slot.role_id()) {
                return Err(self.interrupted(&carrier, Reason::StalePlan));
            }
            let mut context = self.context.clone();
            context.deadline = match (context.deadline, carrier.deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            };
            // A child preserves the parent's fence. Poll guards also retain
            // the additional caller cancellation after ownership is transferred.
            let logical = context.cancel.child_token();
            context.cancel = logical.clone();
            let deadline = context.deadline;
            let admission_deadline = deadline
                .unwrap_or_else(|| tokio::time::Instant::now() + std::time::Duration::from_secs(5));
            let open = async {
                let role = slot.datagram(admission_deadline).await?;
                role.open(request, context).await
            };
            let session = tokio::select! { biased;
                _ = self.context.cancel.cancelled() => return Err(self.interrupted(&carrier,Reason::Cancelled)),
                _ = carrier.cancel.cancelled() => return Err(self.interrupted(&carrier,Reason::Cancelled)),
                result = with_deadline(deadline,open) => result.map_err(|source|self.interrupted(&carrier,Reason::Timeout).with_source(source))?,
            }?;
            // Once open succeeds, its owner and actual Control must be returned
            // even if a generation changes here. New opens will be fenced; no
            // completed carrier is silently discarded as a stale root retry.
            if self.network.network_generation() != binding.network_generation.0 {
                logical.cancel();
            }
            Ok(datagram::guard(session, logical, carrier.cancel))
        })
    }
}

#[cfg(test)]
#[path = "../../tests/relay_prefix.rs"]
mod tests;
