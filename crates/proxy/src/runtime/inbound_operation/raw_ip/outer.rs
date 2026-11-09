//! Optional outer datagram carriers for neutral raw-IP endpoints.

use std::{net::SocketAddr, sync::Arc};
use tokio::sync::mpsc;
use zero_stack::OwnedFragmentReassembler;

use super::{RawIpInboundAction, RawIpInboundEndpoint};
use crate::runtime::raw_ip::RawIpWireCarrier;

pub(super) struct ProxiedWirePacket {
    pub(super) bytes: zero_traits::PacketBuffer,
    pub(super) source: Option<SocketAddr>,
    pub(super) peer: usize,
    pub(super) revision: u64,
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn refresh_endpoint_peers(
    endpoint: Option<&mut RawIpInboundEndpoint>,
    revision: &mut u64,
    initial_endpoints: &mut Vec<Option<SocketAddr>>,
    endpoints: &mut Vec<Option<SocketAddr>>,
    fragments: &mut OwnedFragmentReassembler<(u64, usize)>,
    endpoint_fragments: &mut OwnedFragmentReassembler<(u64, usize)>,
    owner: &super::tasks::RetainedTasks<()>,
    proxied_tasks: &mut super::tasks::TasksGuard<'_, ()>,
    proxied_tx: &mpsc::Sender<ProxiedWirePacket>,
    peer_uses_proxy: &mut Vec<bool>,
    generation: &mut zero_platform_tokio::contract::CancellationToken,
    reads: &[Option<zero_tun::packet_memory::NativeReadGrant>],
    declared: &[Option<Arc<dyn RawIpWireCarrier>>],
) -> Result<(), zero_engine::EngineError> {
    let Some(endpoint) = endpoint else {
        return Ok(());
    };
    let peers = endpoint.peers.borrow_and_update().clone();
    let changed = *revision != peers.revision;
    if !changed && !generation.is_cancelled() {
        return Ok(());
    }
    generation.cancel();
    // Retire one actual reader generation before admitting its replacement;
    // rapid peer-watch changes cannot accumulate aborted but unjoined readers.
    while let Some(joined) = proxied_tasks.join_next().await {
        if let Err(error) = joined {
            if !error.is_cancelled() {
                return Err(std::io::Error::other(owner.remember(error)).into());
            }
        }
    }
    *generation = zero_platform_tokio::contract::CancellationToken::new();
    let carriers = if declared.is_empty() {
        &peers.carriers
    } else {
        declared
    };
    if carriers.len() != peers.carriers.len() {
        return Err(std::io::Error::other("actual Source carrier peer set changed").into());
    }
    *peer_uses_proxy = carriers.iter().map(Option::is_some).collect();
    for (peer, carrier) in carriers.iter().enumerate() {
        let Some(carrier) = carrier.clone() else {
            continue;
        };
        let read = reads
            .get(peer)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| {
                std::io::Error::other(
                    "actual proxy reader has no original prepaid Source workspace",
                )
            })?;
        let stop = generation.clone();
        let sender = proxied_tx.clone();
        let revision = peers.revision;
        proxied_tasks
            .spawn(async move {
                loop {
                    let allocated=tokio::select! {biased;_=stop.cancelled()=>return,result=read.buffer()=>result};
                    let mut buffer=match allocated {Ok(buffer)=>buffer,Err(source)=>{tracing::debug!(?source,peer,"actual Source proxy read workspace stopped");return;}};
                    let received=tokio::select! {biased;_=stop.cancelled()=>return,result=carrier.recv(buffer.as_mut_bytes())=>result};
                    let (size, source) = match received {
                        Ok(value) => value,
                        Err(error) => {
                            tracing::debug!(%error, peer, "raw-IP outer carrier stopped");
                            return;
                        }
                    };
                    let bytes=match buffer.into_packet(size){Ok(bytes)=>bytes,Err(_original)=>return};
                    let delivered=sender.send(ProxiedWirePacket {
                            bytes,
                            source,
                            peer,
                            revision,
                        });
                    // A Ready native receive already transferred this original
                    // buffer. Pause seals the next read, never abandons this
                    // pending delivery. The same wire keeps draining this queue.
                    if delivered.await.is_err() { return; }
                    if stop.is_cancelled() { return; }
                }
            })
            .map_err(std::io::Error::other)?;
    }
    if !changed {
        // Reversible pause restarts only the original reader future over the
        // same carrier. Preserve authenticated endpoints and fragment custody.
        return Ok(());
    }
    *revision = peers.revision;
    // Preserve an authenticated learned address when the configured initial
    // address is unchanged, including a listener-only peer with no address.
    let previous = std::mem::replace(initial_endpoints, peers.initial_endpoints.clone());
    *endpoints = peers
        .initial_endpoints
        .iter()
        .enumerate()
        .map(|(peer, initial)| {
            if previous.get(peer) == Some(initial) {
                endpoints.get(peer).copied().flatten().or(*initial)
            } else {
                *initial
            }
        })
        .collect();
    fragments.reset();
    endpoint_fragments.reset();
    Ok(())
}

pub(super) fn peer_carrier(
    endpoint: &Option<RawIpInboundEndpoint>,
    declared: &[Option<Arc<dyn RawIpWireCarrier>>],
    peer: usize,
) -> Option<Arc<dyn RawIpWireCarrier>> {
    if !declared.is_empty() {
        return declared.get(peer)?.clone();
    }
    endpoint
        .as_ref()?
        .peers
        .borrow()
        .carriers
        .get(peer)?
        .clone()
}

pub(super) async fn send_network_actions(
    socket: &mut zero_platform_tokio::PacketSocketSender,
    carrier: Option<Arc<dyn RawIpWireCarrier>>,
    endpoint: SocketAddr,
    actions: &[RawIpInboundAction],
    traffic: &super::observation::ObservedTraffic,
) {
    for action in actions {
        if let RawIpInboundAction::SendNetwork(packet) = action {
            let result = if let Some(carrier) = &carrier {
                carrier.send(packet, endpoint).await
            } else {
                socket.send_to(packet, endpoint).await.map(|_| ())
            };
            if let Err(error) = result {
                traffic.error(zero_api::TrafficPlane::Outer, true);
                traffic.dropped_reason(
                    zero_api::TrafficPlane::Outer,
                    true,
                    zero_api::TrafficDropReason::IoFailure,
                );
                tracing::debug!(%error, %endpoint, "raw-IP outer datagram send failed");
            } else {
                traffic.tx(zero_api::TrafficPlane::Outer, packet.len());
            }
        }
    }
}
