//! Neutral listener channel and generation lifecycle.
use super::*;
use zero_stack::FragmentReassembler;

pub(super) async fn peers_changed(
    peers: &mut Option<watch::Receiver<Arc<EndpointPeerState>>>,
) -> Result<(), watch::error::RecvError> {
    match peers {
        Some(peers) => peers.changed().await,
        None => std::future::pending().await,
    }
}

pub(super) async fn receive_endpoint_packet(
    endpoint: &mut Option<RawIpInboundEndpoint>,
) -> Option<EndpointPacket> {
    match endpoint {
        Some(endpoint) => endpoint.packets.recv().await,
        None => std::future::pending().await,
    }
}

pub(crate) fn retained_endpoint(
    device: &dyn RawIpInboundDevice,
    initial_endpoints: &[Option<SocketAddr>],
    peer: usize,
) -> Option<SocketAddr> {
    device
        .authenticated_endpoint(peer)
        .or_else(|| initial_endpoints.get(peer).copied().flatten())
}

pub(super) fn refresh_device_generation(
    device: &dyn RawIpInboundDevice,
    generation: &mut u64,
    endpoints: &mut Vec<Option<SocketAddr>>,
    fragments: &mut FragmentReassembler,
    endpoint_fragments: &mut FragmentReassembler,
    initial_endpoints: &[Option<SocketAddr>],
) {
    let current = device.generation();
    if *generation != current {
        *generation = current;
        *endpoints = (0..device.peer_count())
            .map(|peer| retained_endpoint(device, initial_endpoints, peer))
            .collect();
        *fragments = FragmentReassembler::new();
        *endpoint_fragments = FragmentReassembler::new();
    }
}

#[cfg(test)]
mod tests;
