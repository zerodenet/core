//! Explicit untracked library fixtures only; actual Proxy Sources use native grants.
use super::*;
pub(super) fn open_stream<'a>(
    node: &'a NodeOpener,
    binding: &UpstreamBinding,
    destination: Destination,
) -> BoxFuture<'a, Result<BoxStream, ChannelError>> {
    let binding = binding.clone();
    Box::pin(async move {
        if !binding.prefix.is_empty()
            || binding.network_generation.0 != node.generation
            || node.services.network_generation() != node.generation
            || destination != node.endpoint
        {
            return Err(invalid(
                Reason::StalePlan,
                "node opener cannot bypass a prefix, endpoint or network generation",
            ));
        }
        if binding.dial != Default::default() {
            return Err(invalid(
                Reason::InvalidConfig,
                "non-default dial constraints currently require a direct outbound",
            ));
        }
        let host = match &destination.address {
            zero_core::Address::Domain(host) => host.clone(),
            zero_core::Address::Ipv4(ip) => std::net::Ipv4Addr::from(*ip).to_string(),
            zero_core::Address::Ipv6(ip) => std::net::Ipv6Addr::from(*ip).to_string(),
        };
        // The existing node resolver/bootstrap policy is distinct from
        // business destination DNS. No system-DNS/direct fallback is added.
        let socket = node
            .services
            .connect_upstream_owned(host, destination.port)
            .await
            .map_err(|source| {
                let mut error =
                    ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source);
                error.delivery = DeliveryState::NotSubmitted;
                error.retry = zero_platform_tokio::contract::RetryClass::BeforeSubmission;
                error
            })?;
        if node.services.network_generation() != node.generation {
            return Err(invalid(
                Reason::StalePlan,
                "network changed during node establishment",
            ));
        }
        *node.evidence.lock().expect("node evidence poisoned") = Some(Evidence {
            remote: socket.peer_addr().map_err(|source| {
                ChannelError::new(Stage::Dial, Reason::Io, Impact::Session).with_source(source)
            })?,
            local: socket.local_addr().ok(),
        });
        Ok(Box::pin(socket) as BoxStream)
    })
}
