//! Exact query custody selection, separate from a Packet donor's underlay IO.
use super::*;
impl Provider {
    pub(super) fn logical_query_grants(
        &self,
        peer: SocketAddr,
        dial: &zero_traits::DialPolicy,
        datagram: bool,
    ) -> Option<&CarrierGrants> {
        self.roots
            .iter()
            .find(|(root, _)| {
                root.logical
                    && root.datagram == datagram
                    && root.dial == *dial
                    && root.endpoint == destination(peer)
            })
            .map(|(_, grants)| grants)
    }
    pub(super) fn query_root_grants(
        &self,
        endpoint: &zero_core::request::Destination,
        dial: &zero_traits::DialPolicy,
        datagram: bool,
        device: bool,
    ) -> Option<(&CarrierGrants, bool)> {
        if device {
            if let Some((_, grants)) = self.roots.iter().find(|(root, _)| {
                root.logical
                    && root.datagram == datagram
                    && root.dial == *dial
                    && &root.endpoint == endpoint
            }) {
                return Some((grants, true));
            }
        }
        self.roots
            .iter()
            .find(|(root, _)| {
                !root.logical
                    && root.datagram == datagram
                    && root.dial == *dial
                    && &root.endpoint == endpoint
            })
            .map(|(_, grants)| (grants, false))
            .or_else(|| {
                (dial == &Default::default())
                    .then(|| {
                        self.peers
                            .iter()
                            .find(|(peer, packet, _)| {
                                *packet == datagram && &destination(*peer) == endpoint
                            })
                            .map(|(_, _, grants)| (grants, false))
                    })
                    .flatten()
            })
    }
}

// Same existing selection receipt, moved mechanically from the execution loop.
pub(super) fn selected_binding(
    nodes: &[crate::inventory::service::ServiceNode],
    peer: SocketAddr,
    generation: zero_core::component::NetworkGeneration,
    execution: &crate::protocol_registry::TcpExecutionServices,
    selections: &ServiceSelections,
) -> std::io::Result<Arc<dyn zero_dns::DnsCarrierBinding>> {
    let capture = ServiceBinding::new(
        nodes,
        destination(peer),
        generation,
        execution.upstream().network_updates(),
    )
    .map_err(std::io::Error::other)?;
    let selected = selections
        .chosen(&capture.tags())
        .ok_or_else(|| std::io::Error::other("DNS detour selected branch is ambiguous"))?;
    Ok(Arc::new(NativeDnsBinding {
        carrier: capture,
        selections: selected,
    }))
}

pub(super) fn stream_binding(
    nodes: &[crate::inventory::service::ServiceNode],
    network_generation: zero_core::component::NetworkGeneration,
    device: bool,
) -> UpstreamBinding {
    UpstreamBinding {
        prefix: nodes.iter().map(|node| node.slot.role_id()).collect(),
        dial: if device {
            nodes
                .last()
                .map(|node| node.dial.clone())
                .unwrap_or_default()
        } else {
            Default::default()
        },
        network_generation,
    }
}
