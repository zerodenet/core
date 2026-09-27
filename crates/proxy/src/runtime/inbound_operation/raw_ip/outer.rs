//! Optional outer datagram carriers for neutral raw-IP endpoints.

use std::{net::SocketAddr, sync::Arc};
use tokio::{sync::mpsc, task::JoinSet};
use zero_stack::FragmentReassembler;

use super::{RawIpInboundAction, RawIpInboundEndpoint};
use crate::runtime::raw_ip::RawIpWireCarrier;

pub(super) struct ProxiedWirePacket {
    pub(super) bytes: Vec<u8>,
    pub(super) source: Option<SocketAddr>,
    pub(super) peer: usize,
    pub(super) revision: u64,
}

pub(super) fn refresh_endpoint_peers(
    endpoint: Option<&mut RawIpInboundEndpoint>,
    revision: &mut u64,
    initial_endpoints: &mut Vec<SocketAddr>,
    endpoints: &mut Vec<Option<SocketAddr>>,
    fragments: &mut FragmentReassembler,
    endpoint_fragments: &mut FragmentReassembler,
    proxied_tasks: &mut JoinSet<()>,
    proxied_tx: &mpsc::Sender<ProxiedWirePacket>,
    peer_uses_proxy: &mut Vec<bool>,
) {
    let Some(endpoint) = endpoint else {
        return;
    };
    let peers = endpoint.peers.borrow_and_update().clone();
    if *revision == peers.revision {
        return;
    }
    proxied_tasks.abort_all();
    *peer_uses_proxy = peers.carriers.iter().map(Option::is_some).collect();
    for (peer, carrier) in peers.carriers.iter().enumerate() {
        let Some(carrier) = carrier.clone() else {
            continue;
        };
        let sender = proxied_tx.clone();
        let revision = peers.revision;
        proxied_tasks.spawn(async move {
            let mut buffer = vec![0; 65_536];
            loop {
                let (size, source) = match carrier.recv(&mut buffer).await {
                    Ok(value) => value,
                    Err(error) => {
                        tracing::debug!(%error, peer, "raw-IP outer carrier stopped");
                        return;
                    }
                };
                if sender
                    .send(ProxiedWirePacket {
                        bytes: buffer[..size].to_vec(),
                        source,
                        peer,
                        revision,
                    })
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
    }
    *revision = peers.revision;
    *initial_endpoints = peers.initial_endpoints.clone();
    *endpoints = initial_endpoints.iter().copied().map(Some).collect();
    *fragments = FragmentReassembler::new();
    *endpoint_fragments = FragmentReassembler::new();
}

pub(super) fn peer_carrier(
    endpoint: &Option<RawIpInboundEndpoint>,
    peer: usize,
) -> Option<Arc<dyn RawIpWireCarrier>> {
    endpoint
        .as_ref()?
        .peers
        .borrow()
        .carriers
        .get(peer)?
        .clone()
}

pub(super) async fn send_network_actions(
    socket: &zero_platform_tokio::PacketSocket,
    carrier: Option<Arc<dyn RawIpWireCarrier>>,
    endpoint: SocketAddr,
    actions: &[RawIpInboundAction],
) {
    for action in actions {
        if let RawIpInboundAction::SendNetwork(packet) = action {
            let result = if let Some(carrier) = &carrier {
                carrier.send(packet, endpoint).await
            } else {
                socket.send_to(packet, endpoint).await.map(|_| ())
            };
            if let Err(error) = result {
                tracing::debug!(%error, %endpoint, "raw-IP outer datagram send failed");
            }
        }
    }
}
