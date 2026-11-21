//! Explicit standalone stack fixtures. These functions create no native TUN,
//! publish no Source Ready, and issue no native/Scope-close qualification.
//! Ordinary kernel TUN execution is exclusively the owned Packet Source.
use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use zero_engine::EngineError;
use zero_stack::{UserTcpStack, UserUdpStack};
use zero_traits::{TcpStack, UdpStack};

use super::super::sniff::sniff_tcp_target;

type TunTaskResult = (&'static str, Result<(), EngineError>);

async fn shutdown_tun_tasks(tasks: &mut JoinSet<TunTaskResult>) {
    tasks.abort_all();
    while let Some(completed) = tasks.join_next().await {
        match completed {
            Ok((task, result)) => {
                tracing::debug!(?result, "TUN {task} task stopped during shutdown");
            }
            Err(error) if error.is_cancelled() => {}
            Err(error) => tracing::warn!(error = %error, "TUN task failed during shutdown"),
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn feed_packets(
    mut packets: mpsc::Receiver<Vec<u8>>,
    tcp: Arc<UserTcpStack>,
    udp: Arc<UserUdpStack>,
    addresses: Vec<(IpAddr, IpAddr)>,
    mtu: usize,
    network_responses: mpsc::Sender<Vec<u8>>,
    dns_hijack: bool,
    traffic: Option<Arc<dyn zero_traits::IoObserver>>,
    #[cfg(feature = "raw-ip-runtime")]
    packet_route: crate::runtime::route_runtime::InboundRouteRuntimeFactory,
    #[cfg(feature = "raw-ip-runtime")] packet_ingress_id: u64,
    #[cfg(feature = "raw-ip-runtime")] packet_shutdown: watch::Receiver<bool>,
) -> Result<(), EngineError> {
    const PACKET_BATCH_BEFORE_YIELD: usize = 32;
    #[cfg(not(feature = "raw-ip-runtime"))]
    let _ = dns_hijack;
    let mut batch_size = 0;
    let mut fragments = zero_stack::FragmentReassembler::new();
    #[cfg(feature = "raw-ip-runtime")]
    let mut packet_pins = packet_route.packet_statistics_pins();
    #[cfg(feature = "raw-ip-runtime")]
    let packet_responses: zero_stack::packet_output::PacketSender =
        network_responses.clone().into();
    #[cfg(feature = "raw-ip-runtime")]
    let echo = crate::runtime::inbound_operation::raw_ip::IcmpEchoRelay::new(
        packet_responses.clone(),
        packet_route.clone(),
        packet_shutdown,
    );
    let mut route_cleanup = tokio::time::interval(std::time::Duration::from_secs(30));
    route_cleanup.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while let Some(packet) = loop {
        tokio::select! {
            packet = packets.recv() => break packet,
            _ = async {
                #[cfg(feature = "raw-ip-runtime")]
                packet_pins.management_changed().await;
                #[cfg(not(feature = "raw-ip-runtime"))]
                std::future::pending::<()>().await;
            } => {
                #[cfg(feature = "raw-ip-runtime")]
                packet_pins.expire();
            },
            _ = route_cleanup.tick() => {
                fragments.cleanup_expired(std::time::Instant::now());
                #[cfg(feature = "raw-ip-runtime")]
                packet_route.retain_admitted_packet_pins(&mut packet_pins);
            }
        }
    } {
        match fragments.process_owned(packet, std::time::Instant::now()) {
            zero_stack::OwnedFragmentOutcome::Packet {
                packet,
                reassembled,
            } => {
                #[cfg_attr(not(feature = "raw-ip-runtime"), allow(unused_mut))]
                let mut packet: zero_traits::PacketBuffer = packet.into();
                #[cfg(feature = "raw-ip-runtime")]
                let effective_mtu = if reassembled {
                    mtu.max(packet.len())
                } else {
                    mtu
                };
                #[cfg(feature = "raw-ip-runtime")]
                let handled = super::super::packet::try_forward(
                    &mut packet,
                    &packet_route,
                    packet_ingress_id,
                    &mut packet_pins,
                    &packet_responses,
                    &echo,
                    effective_mtu,
                    dns_hijack,
                    traffic.as_deref(),
                )
                .await;
                #[cfg(not(feature = "raw-ip-runtime"))]
                let handled = false;
                if !handled {
                    let response = (!reassembled)
                        .then(|| zero_stack::packet::build_icmp_response(&packet, mtu))
                        .flatten();
                    if let Some(response) = response {
                        if network_responses.send(response).await.is_err() {
                            return Err(EngineError::Io(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "TUN response packet channel closed",
                            )));
                        }
                    } else {
                        feed_transport_packet(&packet, &tcp, &udp, &addresses).await;
                    }
                }
            }
            zero_stack::OwnedFragmentOutcome::Pending => continue,
            zero_stack::OwnedFragmentOutcome::Rejected(reason) => {
                if let Some(traffic) = &traffic {
                    traffic.dropped_reason(zero_traits::PacketDropReason::FragmentRejected);
                }
                tracing::warn!(?reason, "rejected fragmented TUN packet");
                continue;
            }
        }
        batch_size += 1;
        if batch_size == PACKET_BATCH_BEFORE_YIELD {
            batch_size = 0;
            tokio::task::yield_now().await;
        }
    }
    Err(EngineError::Io(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "TUN packet channel closed",
    )))
}

async fn feed_transport_packet(
    packet: &[u8],
    tcp: &UserTcpStack,
    udp: &UserUdpStack,
    addresses: &[(IpAddr, IpAddr)],
) {
    if should_drop_non_unicast_udp(packet, addresses) {
        return;
    }
    tcp.feed(packet).await;
    udp.feed(packet).await;
}

fn should_drop_non_unicast_udp(packet: &[u8], addresses: &[(IpAddr, IpAddr)]) -> bool {
    let Some(datagram) = zero_stack::packet::parse_udp(packet) else {
        return false;
    };
    match datagram.dst.ip {
        IpAddr::V4(destination) => {
            if destination.is_multicast() || destination.is_broadcast() {
                return true;
            }
            addresses.iter().any(|&(address, netmask)| {
                let (IpAddr::V4(address), IpAddr::V4(netmask)) = (address, netmask) else {
                    return false;
                };
                let broadcast = address.to_bits() | !netmask.to_bits();
                destination.to_bits() == broadcast
            })
        }
        IpAddr::V6(destination) => destination.is_multicast(),
    }
}

#[cfg(test)]
#[path = "../tests.rs"]
mod tests;
