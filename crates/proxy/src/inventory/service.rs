//! Resolve the entire selected service path before opening any carrier. Protocol
//! support is determined by actual registered slots, never protocol-name matches.
mod selection;
use super::{PreparedTcpCandidateExecution, ProtocolInventory};
use crate::{
    component::{catalog::ComponentPlan, instance::ComponentSlot},
    protocol_registry::OutboundAdapterContext,
    transport::TcpOutboundFailure,
};
pub(crate) use selection::ServiceSelections;
use std::sync::Arc;
use zero_core::{request::Destination, Address};
use zero_engine::{ResolvedLeafOutbound, ResolvedOutbound};
use zero_platform_tokio::contract::dns::DnsQueryRole;

pub(crate) struct ServiceNode {
    pub(crate) slot: Arc<ComponentSlot>,
    pub(crate) tag: Option<String>,
    /// None only for Direct, whose root endpoint is the requested destination.
    pub(crate) endpoint: Option<Destination>,
    pub(crate) wire_endpoint: Option<Destination>,
    pub(crate) role: DnsQueryRole,
    pub(crate) dial: zero_traits::DialPolicy,
}
pub(crate) enum ServicePlan {
    Single(ServiceNode),
    Relay(Vec<ServiceNode>),
    Fallback(Vec<ServiceNode>),
}

impl ProtocolInventory {
    pub(crate) fn prepare_service_stream(
        &self,
        ctx: OutboundAdapterContext<'_>,
        resolved: &ResolvedOutbound<'_>,
        components: &ComponentPlan,
    ) -> Result<Option<ServicePlan>, TcpOutboundFailure> {
        let node = |leaf| self.service_node(ctx, leaf, components);
        match resolved {
            ResolvedOutbound::Single(leaf) => node(leaf).map(|node| node.map(ServicePlan::Single)),
            ResolvedOutbound::Relay { chain } => {
                let mut nodes = Vec::with_capacity(chain.len());
                for leaf in chain {
                    let Some(node) = node(leaf)? else {
                        return Ok(None);
                    };
                    if node.endpoint.is_none() {
                        return Ok(None);
                    }
                    nodes.push(node);
                }
                Ok(Some(ServicePlan::Relay(nodes)))
            }
            ResolvedOutbound::Fallback { candidates } => {
                let mut nodes = Vec::with_capacity(candidates.len());
                let mut failure = None;
                for leaf in candidates {
                    match node(leaf) {
                        Ok(Some(node)) => nodes.push(node),
                        Ok(None) => return Ok(None),
                        Err(error) => failure = Some(error),
                    }
                }
                if nodes.is_empty() {
                    match failure {
                        Some(error) => Err(error),
                        None => Ok(None),
                    }
                } else {
                    Ok(Some(ServicePlan::Fallback(nodes)))
                }
            }
        }
    }
    fn service_node(
        &self,
        ctx: OutboundAdapterContext<'_>,
        leaf: &ResolvedLeafOutbound<'_>,
        components: &ComponentPlan,
    ) -> Result<Option<ServiceNode>, TcpOutboundFailure> {
        let failure = |error| TcpOutboundFailure {
            stage: "prepare_service",
            error: zero_engine::EngineError::Io(std::io::Error::other(error)),
            upstream_endpoint: None,
            network: None,
        };
        match leaf {
            ResolvedLeafOutbound::Direct {
                tag, dial_policy, ..
            } => {
                let slot = components.direct(*tag).map_err(failure)?;
                Ok(Some(ServiceNode {
                    slot,
                    tag: tag.map(str::to_owned),
                    endpoint: None,
                    wire_endpoint: None,
                    role: DnsQueryRole::Direct,
                    dial: dial_policy.clone(),
                }))
            }
            ResolvedLeafOutbound::Block { .. } => Ok(None),
            ResolvedLeafOutbound::Proxy { .. } => {
                let Some(candidate) = self.native_tcp_candidate(ctx, leaf, Some(components)) else {
                    return Ok(None);
                };
                let candidate = candidate?;
                let PreparedTcpCandidateExecution::Native(slot) = candidate.execution else {
                    return Ok(None);
                };
                let Some((host, port)) = candidate.endpoint else {
                    return Ok(None);
                };
                Ok(Some(ServiceNode {
                    dial: slot.dial.clone(),
                    slot,
                    tag: candidate.tag,
                    endpoint: Some(endpoint(&host, port)),
                    wire_endpoint: Some(Destination::new(Address::Domain(host), port)),
                    role: DnsQueryRole::Node,
                }))
            }
        }
    }
}
pub(crate) fn endpoint(host: &str, port: u16) -> Destination {
    let address = match zero_core::address::parse_ip_literal(host) {
        Some(std::net::IpAddr::V4(ip)) => Address::Ipv4(ip.octets()),
        Some(std::net::IpAddr::V6(ip)) => Address::Ipv6(ip.octets()),
        None => Address::Domain(host.into()),
    };
    Destination::new(address, port)
}
