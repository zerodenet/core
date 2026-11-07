//! Real Stream roles composed through a strictly shorter selected prefix.
pub(crate) mod background;
#[cfg(feature = "managed-stream-runtime")]
pub(crate) mod datagram;
mod health;
pub(crate) mod hop;
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
    ChannelError, Reason,
};

enum RootEvidence {
    Node(Arc<NodeOpener>),
    Direct(Arc<crate::component::direct_bridge::background::Root>),
}
impl RootEvidence {
    fn take(&self) -> Option<(Option<std::net::SocketAddr>, std::net::SocketAddr)> {
        match self {
            Self::Node(root) => root
                .take_evidence()
                .map(|facts| (facts.local, facts.remote)),
            Self::Direct(root) => root.take_evidence(),
        }
    }
}

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
    let normalize = |error: ChannelError| {
        let failed = error
            .component
            .and_then(|id| prepared.nodes.iter().find(|node| node.slot.id == id))
            .unwrap_or(first);
        let mut failure = super::failure_optional("relay_last", error, failed.endpoint.as_ref());
        failure.error = crate::transport::establishment::EstablishmentFailure::wrap(
            failure.error,
            Some(failed.tag.clone()),
            failed.protocol.clone(),
            failed.endpoint.clone(),
            "relay_last",
            None,
        );
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
    let generation = NetworkGeneration(services.upstream().network_generation());
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: first.dial.clone(),
        network_generation: generation,
    };
    let waiting = super::pending::PendingCaller::new(
        services,
        &invocation,
        prepared
            .nodes
            .iter()
            .map(|node| node.slot.role_id())
            .collect(),
        binding.clone(),
    )
    .map_err(normalize)?;
    let (root, mut upstream) = match &first.endpoint {
        Some(endpoint) => {
            let root = Arc::new(
                NodeOpener::new(
                    services.upstream_observed(&first.tag),
                    destination(endpoint),
                )
                .with_plan(services.components.as_ref().ok().map(Arc::downgrade))
                .with_business(
                    crate::component::business_carrier::BusinessCarrierRequest::new(
                        services.engine(),
                        &first.slot,
                        &invocation,
                    )
                    .map_err(normalize)?,
                )
                .with_pending(
                    binding.clone(),
                    waiting
                        .as_ref()
                        .map(super::pending::PendingCaller::observer),
                ),
            );
            (
                Some(RootEvidence::Node(root.clone())),
                UpstreamTransport::new(binding, root),
            )
        }
        None => {
            let plan = services.components.as_ref().map_err(|source| {
                normalize(
                    invalid(Reason::StalePlan, "Packet relay host retired")
                        .with_source(source.clone()),
                )
            })?;
            if first.slot.recipe.spec().resources.iter().any(|resource|matches!(resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream { .. })) {
                let target=prepared.nodes.get(1).and_then(|node|node.endpoint.as_ref()).ok_or_else(||normalize(invalid(Reason::StalePlan,"Direct root has no next-hop endpoint")))?;
                let (root,upstream)=crate::component::direct_bridge::background::Root::transport(
                    &first.slot,Arc::downgrade(plan),services.upstream_observed(&first.tag),destination(target),binding,
                    crate::component::direct_bridge::background::Caller {
                        business:crate::component::business_carrier::BusinessCarrierRequest::new(services.engine(),&first.slot,&invocation).map_err(normalize)?,
                        call:None,waiting:waiting.as_ref().map(super::pending::PendingCaller::observer),
                    },
                ).map_err(normalize)?;
                (Some(RootEvidence::Direct(root)),upstream)
            } else {
            if !plan.has_device_carrier_provider(first.slot.role_id()) {
                return Err(normalize(invalid(
                    Reason::Revoked,
                    "Packet relay original source is not ready",
                )));
            }
            (
                None,
                super::packet_network::business_transport(services, &first.slot, binding)
                    .map_err(normalize)?,
            )
            }
        }
    };
    let cancel = invocation.cancellation();
    let mut prefix = Vec::with_capacity(prepared.nodes.len());
    for (index, (node, role)) in prepared.nodes.iter().zip(roles).enumerate() {
        let context = OpenContext {
            identity: invocation.identity.clone(),
            deadline: invocation.deadline,
            cancel: cancel.clone(),
            observer: if index == 0 && node.endpoint.is_none() && !node.slot.recipe.spec().resources.iter().any(|resource|matches!(resource,zero_platform_tokio::contract::component::ResourceDeclaration::ControlledDirectStream {..})) {
                Some(Arc::new(super::packet_network::PacketObserver {
                    engine: services.engine().clone(),
                    tag: node.tag.clone(),
                    session: invocation.identity.session(),
                    traffic: super::traffic::EstablishmentTraffic::new(None),
                    inner: services.outbound_io(&node.tag, zero_api::TrafficPlane::Inner),
                }))
            } else {
                None
            },
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
            let (local, remote) = match &root {
                Some(root) => {
                    let evidence = root.take().ok_or_else(|| {
                        normalize(invalid(
                            Reason::Internal,
                            "native relay lost the physical carrier evidence",
                        ))
                    })?;
                    (evidence.0, Some(evidence.1))
                }
                None => (None, None),
            };
            // Preserve existing relay session-counter semantics: only the
            // business pump contributes here. Every actual hop still observes
            // its carrier bytes through the existing Outer I/O boundary.
            return Ok(EstablishedTcpOutbound::relay(
                node.tag.clone(),
                first.endpoint.clone(),
                prepared
                    .nodes
                    .iter()
                    .map(|node| (node.tag.clone(), node.protocol.clone()))
                    .collect(),
                zero_platform_tokio::TcpRelayStream::from_client(LegacyStream::with_endpoints(
                    connected.io,
                    local,
                    remote,
                )),
            ));
        };
        let next_endpoint = next.endpoint.as_ref().ok_or_else(|| {
            normalize(invalid(
                Reason::StalePlan,
                "a non-root relay node cannot omit its network endpoint",
            ))
        })?;
        prefix.push(node.slot.role_id());
        let binding = UpstreamBinding {
            prefix: prefix.clone(),
            dial: next.dial.clone(),
            network_generation: generation,
        };
        let plan = services
            .components
            .as_ref()
            .map_err(|source| normalize(crate::component::instance::retained(source.clone())))?;
        let business = crate::component::business_carrier::BusinessCarrierRequest::new(
            services.engine(),
            &next.slot,
            &invocation,
        )
        .map_err(normalize)?
        .map(|request| request.with_prefix(&binding.prefix));
        if business.is_some() {
            plan.authorize_carrier_prefix(next.slot.instance_id(), &binding.prefix)
                .map_err(normalize)?;
        }
        upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(PrefixOpener {
                service_identity: None,
                role,
                business,
                plan: Some(Arc::downgrade(plan)),
                datagram_slot: Some(node.slot.clone()),
                context,
                expected_binding: binding,
                expected_endpoint: destination(next_endpoint),
                // The existing relay passes the configured node name to the
                // previous proxy, including IP literals as named targets. Keep
                // that wire/address form without fabricating a Session(0).
                request_destination: Destination::new(
                    Address::Domain(next_endpoint.0.clone()),
                    next_endpoint.1,
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
