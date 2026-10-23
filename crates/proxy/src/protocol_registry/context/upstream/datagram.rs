//! Explicit root node UDP grant. No prefix or hard dial policy is bypassed.
use super::UpstreamConnectServices;
use zero_core::{request::DatagramTargets, Address};
use zero_platform_tokio::contract::{
    context::with_deadline,
    datagram::{BoxDatagramSession, DatagramRequest},
    network::{CarrierContext, UpstreamBinding},
    ChannelError, DeliveryState, Impact, Reason, Stage,
};
impl UpstreamConnectServices {
    pub(crate) async fn open_node_datagram(
        &self,
        binding: &UpstreamBinding,
        request: DatagramRequest,
        context: CarrierContext,
    ) -> Result<BoxDatagramSession, ChannelError> {
        let failure = |stage, reason| {
            let mut error = ChannelError::new(stage, reason, Impact::Session);
            error.session = context.session;
            error.delivery = DeliveryState::NotSubmitted;
            error
        };
        if !binding.prefix.is_empty() {
            return Err(failure(Stage::Prepare, Reason::Unsupported));
        }
        if binding.network_generation.0 != self.network_generation() {
            return Err(failure(Stage::Prepare, Reason::StalePlan));
        }
        if binding.dial != Default::default() {
            return Err(failure(Stage::Prepare, Reason::Unsupported));
        }
        let DatagramTargets::Fixed(endpoint) = request.targets else {
            // Root node carriers have one actual protocol-selected relay. A
            // business Multiple session belongs to its Direct/proxy role.
            return Err(failure(Stage::Prepare, Reason::Unsupported));
        };
        let host = match endpoint.address {
            Address::Domain(host) => host,
            Address::Ipv4(ip) => std::net::Ipv4Addr::from(ip).to_string(),
            Address::Ipv6(ip) => std::net::Ipv6Addr::from(ip).to_string(),
        };
        let factory = self.outbound_datagram_socket_factory();
        let establish = async {
            // Existing node/bootstrap resolver, egress, and I/O observer are
            // retained. A missing configured resolver never falls back.
            let addresses = factory
                .resolve_server_addresses(&host, endpoint.port)
                .await
                .map_err(|source| failure(Stage::Dial, Reason::Io).with_source(source))?;
            let peer = addresses[0];
            let socket = factory
                .bind_tokio(peer)
                .await
                .map_err(|source| failure(Stage::Dial, Reason::Io).with_source(source))?;
            if binding.network_generation.0 != self.network_generation() {
                return Err(failure(Stage::Dial, Reason::StalePlan));
            }
            Ok(socket.into_fixed_datagram_session(
                request.association,
                peer,
                context.cancel.clone(),
            ))
        };
        tokio::select! { biased;
            _ = context.cancel.cancelled() => Err(failure(Stage::Dial, Reason::Cancelled)),
            result = with_deadline(context.deadline, establish) => result.map_err(|source|failure(Stage::Dial, Reason::Timeout).with_source(source))?,
        }
    }
}
