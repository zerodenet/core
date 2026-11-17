//! Native service requests carry a Destination and real Service identity all the way
//! through their selected prefix. Unsupported plans are identified before I/O.
mod binding;
mod datagram;
pub(crate) use datagram::connect_carrier_datagram;
mod receipt;
pub(crate) use binding::NativeDnsBinding;
#[cfg(test)]
#[path = "service/tests/legacy_connect.rs"]
mod legacy_connect;
#[path = "service/ingress_root.rs"]
mod root;
use crate::{
    component::{instance::retained, stream_bridge::relay::prefix::PrefixOpener},
    inventory::{
        service::{ServiceNode, ServicePlan},
        ProtocolInventory,
    },
    protocol_registry::{OutboundAdapterContext, TcpExecutionServices, TcpRuntimeServices},
    transport::TcpOutboundFailure,
};
#[cfg(test)]
pub(crate) use legacy_connect::connect;
pub(crate) use receipt::ServiceBinding;
#[cfg(test)]
pub(crate) use receipt::ServiceConnected;
use std::{sync::Arc, time::Duration};
use zero_core::{
    component::NetworkGeneration,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_engine::ResolvedOutbound;
use zero_platform_tokio::contract::{
    context::{OpenContext, RequestIdentity},
    network::{CarrierContext, UpstreamBinding, UpstreamTransport},
    stream::ConnectRequest,
    ChannelError, DeliveryState, Impact, Reason, Stage,
};

pub(crate) fn prepare(
    protocols: &ProtocolInventory,
    services: &TcpRuntimeServices,
    resolved: &ResolvedOutbound<'_>,
) -> Result<Option<ServicePlan>, TcpOutboundFailure> {
    let execution = services.execution();
    let components = execution
        .components
        .as_ref()
        .map_err(|source| TcpOutboundFailure {
            stage: "prepare_service",
            error: zero_engine::EngineError::Io(std::io::Error::other(retained(source.clone()))),
            upstream_endpoint: None,
            network: None,
        })?;
    protocols.prepare_service_stream(
        OutboundAdapterContext::new(services.config()),
        resolved,
        components,
    )
}

/// Active ingress borrows the actual selected generation and its original
/// provider. The protocol Source retains the returned logical controller.
pub(crate) async fn connect_ingress(
    services: &TcpExecutionServices,
    destination: &Destination,
    node: ServiceNode,
    call: &CarrierContext,
) -> Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError> {
    if call.session.is_some() || node.endpoint.as_ref() != Some(destination) {
        return Err(failure(Reason::StalePlan));
    }
    let network = node.tag.as_ref().map_or_else(
        || services.upstream(),
        |tag| services.upstream_observed(tag),
    );
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: node.dial.clone(),
        network_generation: NetworkGeneration(network.network_generation()),
    };
    let upstream = UpstreamTransport::new(
        binding.clone(),
        Arc::new(root::RootOpener {
            services: network,
            binding,
            endpoint: destination.clone(),
            #[cfg(test)]
            role: node.role,
            #[cfg(test)]
            evidence: Arc::default(),
            source: services.current_source.clone(),
            selected: node.slot.role_id(),
            call: call.clone(),
            candidates: tokio::sync::Mutex::new(None),
        }),
    );
    connect_carrier_stream(
        services,
        destination,
        &[node],
        ServicePurpose::DialedIngress,
        call,
        upstream,
    )
    .await
}

/// Execute the actual standard roles over the caller's original I/O provider.
/// Protocol handshakes and relay framing share this one execution loop.
pub(crate) async fn connect_stream(
    services: &TcpExecutionServices,
    destination: &Destination,
    nodes: &[ServiceNode],
    purpose: ServicePurpose,
    call: &CarrierContext,
    upstream: UpstreamTransport,
) -> Result<zero_platform_tokio::contract::stream::BoxStream, ChannelError> {
    let (role, context) = prepare_stream_path(services, nodes, purpose, call, upstream).await?;
    Ok(role
        .connect(
            ConnectRequest {
                destination: destination.clone(),
            },
            context,
        )
        .await?
        .io)
}

/// Transfer the selected role's actual logical control and original lineage.
/// The caller retains the returned owner; native retirement stays with its donor.
pub(crate) async fn connect_carrier_stream(
    services: &TcpExecutionServices,
    destination: &Destination,
    nodes: &[ServiceNode],
    purpose: ServicePurpose,
    call: &CarrierContext,
    upstream: UpstreamTransport,
) -> Result<zero_platform_tokio::controlled_carrier::loan::StreamLoan, ChannelError> {
    let (role, context) = prepare_stream_path(services, nodes, purpose, call, upstream).await?;
    Ok(role
        .connect_carrier(
            ConnectRequest {
                destination: destination.clone(),
            },
            context,
        )
        .await?
        .io)
}

async fn prepare_stream_path(
    services: &TcpExecutionServices,
    nodes: &[ServiceNode],
    purpose: ServicePurpose,
    call: &CarrierContext,
    mut upstream: UpstreamTransport,
) -> Result<
    (
        Arc<dyn zero_platform_tokio::contract::stream::DynStreamOutbound>,
        OpenContext,
    ),
    ChannelError,
> {
    let start_deadline = call
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5));
    let mut roles = Vec::with_capacity(nodes.len());
    for node in nodes {
        roles.push(node.slot.stream(start_deadline).await?);
    }
    let generation = upstream.binding().network_generation;
    let identity = RequestIdentity::Service(
        Arc::new(ServiceRequestFacts {
            origin: ServiceOrigin::Runtime,
            purpose,
        })
        .into(),
    );
    let prefix_identity = if purpose == ServicePurpose::DialedIngress && nodes.len() > 1 {
        RequestIdentity::Service(
            Arc::new(ServiceRequestFacts {
                origin: ServiceOrigin::Runtime,
                purpose: ServicePurpose::BackgroundControl,
            })
            .into(),
        )
    } else {
        identity.clone()
    };
    let mut prefix = Vec::with_capacity(nodes.len());
    for (index, (node, role)) in nodes.iter().zip(roles).enumerate() {
        // A Service receiver is a real transitive consumer just like a
        // Business receiver. Register that dependency before its acquisition,
        // so replacement drains it before retiring the shorter prefix owner.
        if !prefix.is_empty() && (services.components.as_ref().is_ok_and(|plan| plan.has_stream_loan_provider(node.slot.role_id())) || node.slot.recipe.spec().resources.iter().any(|resource| matches!(resource,
            zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool { .. }
            | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledAssociationPool { .. }
            | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeCarrier { .. }
            | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledNodeStream { .. }
            | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledServiceStream { .. }
            | zero_platform_tokio::contract::component::ResourceDeclaration::ControlledMetadataCarrier { .. }))) {
            services.components.as_ref().map_err(|source| {
                failure(Reason::Revoked).with_source(retained(source.clone()))
            })?.authorize_carrier_prefix(node.slot.instance_id(), &prefix)?;
        }
        let context = OpenContext {
            // Only the terminal role speaks the active ingress protocol. A
            // VLESS prefix still opens an ordinary tunnel to that real peer.
            identity: if index + 1 == nodes.len() {
                identity.clone()
            } else {
                prefix_identity.clone()
            },
            deadline: call.deadline,
            cancel: call.cancel.clone(),
            observer: None,
            upstream,
        };
        let Some(next) = nodes.get(index + 1) else {
            return Ok((role, context));
        };
        let endpoint = next
            .endpoint
            .clone()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        let wire = next
            .wire_endpoint
            .clone()
            .ok_or_else(|| failure(Reason::InvalidConfig))?;
        prefix.push(node.slot.role_id());
        let binding = UpstreamBinding {
            prefix: prefix.clone(),
            dial: next.dial.clone(),
            network_generation: generation,
        };
        let observer = next
            .tag
            .as_ref()
            .and_then(|tag| services.outbound_io(tag, zero_api::TrafficPlane::Outer));
        upstream = UpstreamTransport::new(
            binding.clone(),
            Arc::new(PrefixOpener::for_service(
                (node, role),
                context,
                binding,
                endpoint,
                wire,
                services,
                observer,
            )?),
        );
    }
    Err(failure(Reason::Internal))
}
fn failure(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
#[cfg(test)]
fn interrupted(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Dial, reason, Impact::Session)
}
