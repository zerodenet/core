use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::EngineError;
use zero_stack::{UserTcpStack, UserTcpStream, UserUdpStack};
use zero_traits::{TcpStack, UdpStack};

use crate::runtime::tcp_ingress::{InboundProtocol, TcpIngressRuntime};
use crate::runtime::Proxy;
use crate::transport::ReplayStream;

use super::sniff::sniff_tcp_target;

const TCP_STATE_IDLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);
const TCP_STATE_CLEANUP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);
const MAX_CONCURRENT_DNS_CONNECTIONS: usize = 256;

struct TunProtocol;

pub(super) struct TunIngressConfig {
    pub addresses: Vec<(IpAddr, IpAddr)>,
    pub tag: String,
    pub dns_hijack: bool,
    pub mtu: usize,
    pub network_responses: mpsc::Sender<Vec<u8>>,
}

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

pub(super) async fn run(
    proxy: Proxy,
    packets: mpsc::Receiver<Vec<u8>>,
    tcp: Arc<UserTcpStack>,
    udp: Arc<UserUdpStack>,
    config: TunIngressConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), EngineError> {
    let TunIngressConfig {
        addresses,
        tag,
        dns_hijack,
        mtu,
        network_responses,
    } = config;
    let mut packet_task = tokio::spawn(feed_packets(
        packets,
        Arc::clone(&tcp),
        Arc::clone(&udp),
        addresses,
        mtu,
        network_responses,
    ));
    let mut tcp_task = tokio::spawn(accept_tcp(
        proxy.clone(),
        Arc::clone(&tcp),
        tag.clone(),
        dns_hijack,
    ));
    let mut tcp_maintenance_task = tokio::spawn(maintain_tcp_state(tcp));

    #[cfg(feature = "udp-runtime")]
    let mut udp_task = tokio::spawn(super::udp::run(proxy, udp, tag, dns_hijack));
    #[cfg(not(feature = "udp-runtime"))]
    let mut udp_task = tokio::spawn(std::future::pending::<Result<(), EngineError>>());

    let result = tokio::select! {
        changed = shutdown.changed() => {
            tracing::debug!(?changed, requested = *shutdown.borrow(), "TUN runtime received shutdown signal");
            Ok(())
        }
        result = &mut packet_task => {
            tracing::debug!(?result, "TUN packet loop exited");
            flatten_task_result(result, "packet")
        },
        result = &mut tcp_task => {
            tracing::debug!(?result, "TUN TCP loop exited");
            flatten_task_result(result, "TCP")
        },
        result = &mut tcp_maintenance_task => {
            tracing::debug!(?result, "TUN TCP maintenance loop exited");
            flatten_task_result(result, "TCP maintenance")
        },
        result = &mut udp_task => {
            tracing::debug!(?result, "TUN UDP loop exited");
            flatten_task_result(result, "UDP")
        },
    };
    packet_task.abort();
    tcp_task.abort();
    tcp_maintenance_task.abort();
    udp_task.abort();
    result
}

async fn maintain_tcp_state(tcp: Arc<UserTcpStack>) -> Result<(), EngineError> {
    let mut interval = tokio::time::interval(TCP_STATE_CLEANUP_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        tcp.cleanup_idle(TCP_STATE_IDLE_TIMEOUT).await;
    }
}

fn flatten_task_result(
    result: Result<Result<(), EngineError>, tokio::task::JoinError>,
    task: &str,
) -> Result<(), EngineError> {
    result.unwrap_or_else(|error| {
        Err(EngineError::Io(io::Error::other(format!(
            "TUN {task} task failed: {error}"
        ))))
    })
}

async fn feed_packets(
    mut packets: mpsc::Receiver<Vec<u8>>,
    tcp: Arc<UserTcpStack>,
    udp: Arc<UserUdpStack>,
    addresses: Vec<(IpAddr, IpAddr)>,
    mtu: usize,
    network_responses: mpsc::Sender<Vec<u8>>,
) -> Result<(), EngineError> {
    const PACKET_BATCH_BEFORE_YIELD: usize = 32;
    let mut batch_size = 0;
    let mut fragments = zero_stack::FragmentReassembler::new();
    while let Some(packet) = packets.recv().await {
        match fragments.process(&packet, std::time::Instant::now()) {
            zero_stack::FragmentOutcome::NotFragmented(packet) => {
                if let Some(response) = zero_stack::packet::build_icmp_response(packet, mtu) {
                    if network_responses.send(response).await.is_err() {
                        return Err(EngineError::Io(io::Error::new(
                            io::ErrorKind::BrokenPipe,
                            "TUN response packet channel closed",
                        )));
                    }
                } else {
                    feed_transport_packet(packet, &tcp, &udp, &addresses).await;
                }
            }
            zero_stack::FragmentOutcome::Reassembled(packet) => {
                feed_transport_packet(&packet, &tcp, &udp, &addresses).await;
            }
            zero_stack::FragmentOutcome::Pending => continue,
            zero_stack::FragmentOutcome::Rejected(reason) => {
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
    proxy: Proxy,
    tcp: Arc<UserTcpStack>,
    tag: String,
    dns_hijack: bool,
) -> Result<(), EngineError> {
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
                    if dns_connections.len() >= MAX_CONCURRENT_DNS_CONNECTIONS {
                        tracing::warn!(
                            active_dns_connections = dns_connections.len(),
                            "rejecting TUN DNS TCP connection at the concurrency limit"
                        );
                        continue;
                    }
                    let resolver = Arc::clone(&proxy.resolver);
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
                    proxy.tcp_runtime_services(),
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
                    Ok(Err(error)) => tracing::warn!(error = %error, "TUN TCP connection failed"),
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

async fn serve_dns_tcp(
    resolver: Arc<zero_dns::DnsSystem>,
    mut stream: UserTcpStream,
) -> io::Result<()> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    loop {
        let mut length = [0_u8; 2];
        match stream.read_exact(&mut length).await {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        }
        let length = u16::from_be_bytes(length) as usize;
        if length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-length DNS-over-TCP query",
            ));
        }
        let mut query = vec![0_u8; length];
        stream.read_exact(&mut query).await?;
        let response = resolver.answer_tcp_query(&query).await?;
        let response_length: u16 = response.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "DNS-over-TCP response is too large",
            )
        })?;
        stream.write_all(&response_length.to_be_bytes()).await?;
        stream.write_all(&response).await?;
        stream.flush().await?;
    }
}

fn socket_address_to_address(address: zero_traits::SocketAddress) -> Address {
    match address.ip {
        zero_traits::IpAddress::V4(ip) => Address::Ipv4(ip),
        zero_traits::IpAddress::V6(ip) => Address::Ipv6(ip),
    }
}

#[cfg(test)]
mod tests;
