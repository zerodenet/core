//! Native service requests carry a Destination and real Service identity all the way
//! through their selected prefix. Unsupported plans are identified before I/O.
mod receipt;
mod root;
use crate::{
    component::{
        instance::retained, legacy_stream::LegacyStream, stream_bridge::relay::prefix::PrefixOpener,
    },
    inventory::{
        service::{ServiceNode, ServicePlan},
        ProtocolInventory,
    },
    protocol_registry::{OutboundAdapterContext, TcpExecutionServices, TcpRuntimeServices},
    transport::TcpOutboundFailure,
};
pub(crate) use receipt::{ServiceBinding, ServiceConnected};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use zero_core::{
    component::NetworkGeneration,
    request::{Destination, ServiceOrigin, ServicePurpose, ServiceRequestFacts},
};
use zero_engine::ResolvedOutbound;
use zero_platform_tokio::{
    contract::{
        context::{with_deadline, OpenContext, RequestIdentity},
        network::{CarrierContext, UpstreamBinding, UpstreamTransport},
        stream::ConnectRequest,
        ChannelError, DeliveryState, Impact, Reason, Stage,
    },
    TcpRelayStream,
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

pub(crate) async fn connect(
    services: TcpExecutionServices,
    destination: Destination,
    plan: ServicePlan,
    purpose: ServicePurpose,
    context: CarrierContext,
) -> Result<ServiceConnected, ChannelError> {
    if context.session.is_some() {
        return Err(failure(Reason::InvalidConfig));
    }
    if context.cancel.is_cancelled() {
        return Err(failure(Reason::Cancelled));
    }
    if context
        .deadline
        .is_some_and(|end| tokio::time::Instant::now() >= end)
    {
        return Err(failure(Reason::Timeout));
    }
    let operation = async {
        match plan {
            ServicePlan::Single(node) => {
                chain(&services, &destination, vec![node], purpose, &context).await
            }
            ServicePlan::Relay(nodes) => {
                chain(&services, &destination, nodes, purpose, &context).await
            }
            ServicePlan::Fallback(nodes) => {
                let mut last = None;
                for node in nodes {
                    match chain(&services, &destination, vec![node], purpose, &context).await {
                        Ok(io) => return Ok(io),
                        Err(error) => {
                            if !error.retry_permitted() {
                                return Err(error);
                            }
                            last = Some(error);
                        }
                    }
                }
                Err(last.unwrap_or_else(|| failure(Reason::InvalidConfig)))
            }
        }
    };
    tokio::select! {biased;
        _=context.cancel.cancelled()=>Err(interrupted(Reason::Cancelled)),
        result=with_deadline(context.deadline,operation)=>result.map_err(|source|interrupted(Reason::Timeout).with_source(source))?,
    }
}

async fn chain(
    services: &TcpExecutionServices,
    destination: &Destination,
    nodes: Vec<ServiceNode>,
    purpose: ServicePurpose,
    call: &CarrierContext,
) -> Result<ServiceConnected, ChannelError> {
    let first = nodes
        .first()
        .ok_or_else(|| failure(Reason::InvalidConfig))?;
    let start_deadline = call
        .deadline
        .unwrap_or_else(|| tokio::time::Instant::now() + Duration::from_secs(5));
    let mut roles = Vec::with_capacity(nodes.len());
    for node in &nodes {
        roles.push(node.slot.stream(start_deadline).await?);
    }
    let physical = first
        .endpoint
        .clone()
        .unwrap_or_else(|| destination.clone());
    let network = match &first.tag {
        Some(tag) => services.upstream_observed(tag),
        None => services.upstream(),
    };
    let generation = NetworkGeneration(network.network_generation());
    let network_updates = network.network_updates();
    let binding = UpstreamBinding {
        prefix: vec![],
        dial: first.dial.clone(),
        network_generation: generation,
    };
    let root = Arc::new(root::RootOpener {
        services: network,
        binding: binding.clone(),
        endpoint: physical,
        role: first.role,
        evidence: Mutex::new(None),
    });
    let mut upstream = UpstreamTransport::new(binding, root.clone());
    let identity = RequestIdentity::Service(Arc::new(ServiceRequestFacts {
        origin: ServiceOrigin::Runtime,
        purpose,
    }));
    let mut prefix = Vec::with_capacity(nodes.len());
    for (index, (node, role)) in nodes.iter().zip(roles).enumerate() {
        let context = OpenContext {
            identity: identity.clone(),
            deadline: call.deadline,
            cancel: call.cancel.clone(),
            observer: None,
            upstream,
        };
        let Some(next) = nodes.get(index + 1) else {
            let connected = role
                .connect(
                    ConnectRequest {
                        destination: destination.clone(),
                    },
                    context,
                )
                .await?;
            let evidence = root
                .take_evidence()
                .ok_or_else(|| failure(Reason::Internal))?;
            let binding =
                ServiceBinding::new(&nodes, destination.clone(), generation, network_updates)?;
            return Ok(ServiceConnected {
                stream: TcpRelayStream::from_client(LegacyStream::new(
                    connected.io,
                    Some(evidence.local),
                    evidence.peer,
                )),
                binding,
            });
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
                role,
                context,
                binding,
                endpoint,
                wire,
                services.upstream(),
                observer,
            )),
        );
    }
    Err(failure(Reason::Internal))
}
fn failure(reason: Reason) -> ChannelError {
    let mut error = ChannelError::new(Stage::Prepare, reason, Impact::Session);
    error.delivery = DeliveryState::NotSubmitted;
    error
}
fn interrupted(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Dial, reason, Impact::Session)
}
