//! Real Stream roles composed through a strictly shorter selected prefix.
mod health;
pub(crate) mod prefix;
use super::{network::NodeOpener, LegacyStream};
use crate::{
    component::instance::invalid,
    inventory::PreparedNativeTcpRelayChain,
    protocol_registry::TcpExecutionServices,
    transport::{EstablishedTcpOutbound, TcpOutboundFailure},
};
use health::FirstHopHealth;
use prefix::PrefixOpener;
use std::{sync::Arc, time::Duration};
use zero_core::{component::NetworkGeneration, request::Destination, Address, Session};
use zero_platform_tokio::contract::{
    context::OpenContext,
    network::{UpstreamBinding, UpstreamTransport},
    stream::ConnectRequest,
    Reason,
};

pub(crate) async fn connect(
    services: &TcpExecutionServices,
    session: &Session,
    prepared: PreparedNativeTcpRelayChain,
    record_health: bool,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let first = prepared
        .nodes
        .first()
        .expect("validated relay has two nodes");
    let normalize = |error| {
        let mut failure = super::failure("relay_last", error, &first.endpoint);
        failure.upstream_endpoint = None;
        failure
    };
    let invocation = services.invocation.clone().ok_or_else(|| {
        normalize(invalid(
            Reason::Internal,
            "native relay requires explicit request identity",
        ))
    })?;
    let start_deadline = invocation
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5));
    // Resolve every actual role before acquiring even the first carrier. A
    // revoked middle role cannot accidentally escape through a legacy claim.
    let mut roles = Vec::with_capacity(prepared.nodes.len());
    for node in &prepared.nodes {
        roles.push(
            node.slot
                .stream(start_deadline)
                .await
                .map_err(|mut error| {
                    error.component.get_or_insert(node.slot.id);
                    error.session = error.session.or(invocation.identity.session());
                    normalize(error)
                })?,
        );
    }
    let root = Arc::new(NodeOpener::new(
        services.upstream_observed(&first.tag),
        destination(&first.endpoint),
    ));
    let generation = NetworkGeneration(root.generation());
    let mut upstream = UpstreamTransport::new(
        UpstreamBinding {
            prefix: vec![],
            dial: first.dial.clone(),
            network_generation: generation,
        },
        root.clone(),
    );
    let cancel = invocation.cancellation();
    let mut prefix = Vec::with_capacity(prepared.nodes.len());
    for (index, (node, role)) in prepared.nodes.iter().zip(roles).enumerate() {
        let context = OpenContext {
            identity: invocation.identity.clone(),
            deadline: invocation.deadline,
            cancel: cancel.clone(),
            observer: None,
            upstream,
        };
        let Some(next) = prepared.nodes.get(index + 1) else {
            let connected = role
                .connect(
                    ConnectRequest {
                        destination: Destination::new(session.target.clone(), session.port),
                    },
                    context,
                )
                .await
                .map_err(normalize)?;
            let evidence = root.take_evidence().ok_or_else(|| {
                normalize(invalid(
                    Reason::Internal,
                    "native relay lost the physical carrier evidence",
                ))
            })?;
            // Preserve existing relay session-counter semantics: only the
            // business pump contributes here. Every actual hop still observes
            // its carrier bytes through the existing Outer I/O boundary.
            return Ok(EstablishedTcpOutbound::relay(
                node.tag.clone(),
                Some(first.endpoint.clone()),
                prepared
                    .nodes
                    .iter()
                    .map(|node| (node.tag.clone(), node.protocol.clone()))
                    .collect(),
                zero_platform_tokio::TcpRelayStream::from_client(LegacyStream::new(
                    connected.io,
                    evidence.local,
                    evidence.remote,
                )),
            ));
        };
        prefix.push(node.slot.role_id());
        let binding = UpstreamBinding {
            prefix: prefix.clone(),
            dial: next.dial.clone(),
            network_generation: generation,
        };
        upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(PrefixOpener {
                role,
                datagram_slot: Some(node.slot.clone()),
                context,
                expected_binding: binding,
                expected_endpoint: destination(&next.endpoint),
                // The existing relay passes the configured node name to the
                // previous proxy, including IP literals as named targets. Keep
                // that wire/address form without fabricating a Session(0).
                request_destination: Destination::new(
                    Address::Domain(next.endpoint.0.clone()),
                    next.endpoint.1,
                ),
                network: services.upstream(),
                observer: services.outbound_io(&next.tag, zero_api::TrafficPlane::Outer),
                health: (index == 0 && record_health).then(|| {
                    FirstHopHealth::new(services.engine(), &services.snapshot_handle(), &node.tag)
                }),
            }),
        );
    }
    Err(normalize(invalid(
        Reason::Internal,
        "native relay has no final role",
    )))
}

fn destination(endpoint: &(String, u16)) -> Destination {
    let address = match zero_core::address::parse_ip_literal(&endpoint.0) {
        Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
        Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
        None => Address::Domain(endpoint.0.clone()),
    };
    Destination::new(address, endpoint.1)
}
