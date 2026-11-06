//! Explicit standalone stack fixtures. These functions create no native TUN,
//! publish no Source Ready, and issue no native/Scope-close qualification.
//! Ordinary kernel TUN execution is exclusively the owned Packet Source.
use std::io;
use std::net::IpAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::EngineError;
use zero_stack::{UserTcpStack, UserTcpStream, UserUdpStack};
use zero_traits::{TcpStack, UdpStack};

use crate::runtime::tcp_ingress::{InboundProtocol, TcpIngressRuntime};
use crate::transport::ReplayStream;

use super::super::sniff::sniff_tcp_target;

use super::{serve_dns_tcp, MAX_CONCURRENT_DNS_CONNECTIONS};
type TunTaskResult = (&'static str, Result<(), EngineError>);

struct TunProtocol;

#[async_trait]
impl InboundProtocol for TunProtocol {
    type ClientStream = ReplayStream<UserTcpStream>;

    async fn send_ok(&self, _: &mut Self::ClientStream) -> Result<(), EngineError> {
        Ok(())
    }

    async fn send_blocked(&self, client: &mut Self::ClientStream) -> Result<(), EngineError> {
        use tokio::io::AsyncWriteExt;
        client.shutdown().await.map_err(EngineError::Io)
    }

    async fn send_upstream_failure(
        &self,
        client: &mut Self::ClientStream,
    ) -> Result<(), EngineError> {
        use tokio::io::AsyncWriteExt;
        client.shutdown().await.map_err(EngineError::Io)
    }
}

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

async fn accept_tcp(
    services: crate::protocol_registry::TcpRuntimeServices,
    tcp: Arc<UserTcpStack>,
    tag: String,
    dns_hijack: bool,
    dns_hijacked_queries: Arc<AtomicU64>,
) -> Result<(), EngineError> {
    let services = services.for_inbound(tag.clone());
    let mut connections = JoinSet::new();
    let mut dns_connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = tcp.accept() => {
                let Some((stream, source, destination)) = accepted else {
                    return Err(EngineError::Io(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "TUN TCP stack closed",
                    )));
                };
                let source_addr = zero_platform_tokio::socket_address_to_socket_addr(source);
                if dns_hijack && destination.port == 53 {
                    dns_hijacked_queries.fetch_add(1, Ordering::Relaxed);
                    if dns_connections.len() >= MAX_CONCURRENT_DNS_CONNECTIONS {
                        tracing::warn!(
                            active_dns_connections = dns_connections.len(),
                            "rejecting TUN DNS TCP connection at the concurrency limit"
                        );
                        continue;
                    }
                    let resolver = services.resolver_handle();
                    dns_connections.spawn(async move {
                        serve_dns_tcp(resolver, stream).await.map_err(EngineError::Io)
                    });
                    continue;
                }
                let mut session = Session::new(
                    0,
                    socket_address_to_address(destination),
                    destination.port,
                    Network::Tcp,
                    ProtocolType::UNKNOWN,
                );
                session.transparent_target = true;
                let runtime = TcpIngressRuntime::new(
                    services.with_current_snapshot(),
                    tag.clone(),
                    Some(source_addr),
                );
                connections.spawn(async move {
                    let (session, stream) = sniff_tcp_target(session, stream).await;
                    runtime.serve(session, stream, &TunProtocol).await
                });
            }
            Some(completed) = connections.join_next(), if !connections.is_empty() => {
                match completed {
                    Ok(Ok(())) => {}
                    // The session runtime already emits the structured,
                    // rate-limited failure record. Keep this join result at
                    // debug so one client error cannot produce two warnings.
                    Ok(Err(error)) => tracing::debug!(error = %error, "TUN TCP connection failed"),
                    Err(error) => tracing::warn!(error = %error, "TUN TCP connection task panicked"),
                }
            }
            Some(completed) = dns_connections.join_next(), if !dns_connections.is_empty() => {
                match completed {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(error = %error, "TUN DNS TCP connection failed"),
                    Err(error) => tracing::warn!(error = %error, "TUN DNS TCP task panicked"),
                }
            }
        }
    }
}

fn socket_address_to_address(address: zero_traits::SocketAddress) -> Address {
    match address.ip {
        zero_traits::IpAddress::V4(ip) => Address::Ipv4(ip),
        zero_traits::IpAddress::V6(ip) => Address::Ipv6(ip),
    }
}

#[cfg(test)]
#[path = "../tests.rs"]
mod tests;
