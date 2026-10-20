//! Packet-path execution projected into a reusable carrier socket factory.
use crate::protocol_registry::{
    PacketPathExecutionServices, TcpExecutionServices, UdpNetworkServices,
};
use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;
use crate::runtime::udp_flow::packet_path::PacketPathCarrier;
use std::{io, sync::Arc};

pub(super) fn factory(
    services: &TcpExecutionServices,
    retained: super::retained::RetainedSource,
    prefix: crate::inventory::PreparedTcpRelayPrefix,
    plan: PreparedDatagramRelayCarrier,
) -> zero_transport::OutboundDatagramSocketFactory {
    let network = UdpNetworkServices::from_tcp_execution(services);
    let factory = network.outbound_datagram_socket_factory();
    let factory = if matches!(retained, super::retained::RetainedSource::Bound(_)) {
        factory.with_host_resolver(Arc::new(RetainedResolver {
            retained: retained.clone(),
            prefix: prefix.clone(),
        }))
    } else {
        factory
    };
    factory.with_relay(Arc::new(move |peer| {
        let retained = retained.clone();
        let prefix = prefix.clone();
        let plan = plan.clone();
        Box::pin(async move {
            let services = retained.services(&prefix).map_err(io::Error::other)?;
            let execution = PacketPathExecutionServices::from_tcp_execution(&services);
            let carrier = plan.build(execution).await.map_err(io::Error::other)?;
            Ok(Arc::new(Connected {
                carrier,
                target: match peer.ip() {
                    std::net::IpAddr::V4(ip) => zero_core::Address::Ipv4(ip.octets()),
                    std::net::IpAddr::V6(ip) => zero_core::Address::Ipv6(ip.octets()),
                },
                port: peer.port(),
            })
                as Arc<
                    dyn zero_transport::datagram_relay::ConnectedDatagram,
                >)
        })
    }))
}

/// Node resolution also takes one short host borrow; the retained factory must
/// not preserve the old NodeHostResolver's strong DNS-system handle.
struct RetainedResolver {
    retained: super::retained::RetainedSource,
    prefix: crate::inventory::PreparedTcpRelayPrefix,
}
impl std::fmt::Debug for RetainedResolver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RetainedRelayResolver")
    }
}
impl zero_transport::OutboundHostResolver for RetainedResolver {
    fn resolve(&self, host: String, port: u16) -> zero_transport::OutboundHostResolveFuture {
        let retained = self.retained.clone();
        let prefix = self.prefix.clone();
        Box::pin(async move {
            retained
                .services(&prefix)
                .map_err(io::Error::other)?
                .upstream()
                .outbound_datagram_socket_factory()
                .resolve_server_addresses(&host, port)
                .await
        })
    }
}

struct Connected {
    carrier: Arc<dyn PacketPathCarrier>,
    target: zero_core::Address,
    port: u16,
}
#[async_trait::async_trait]
impl zero_transport::datagram_relay::ConnectedDatagram for Connected {
    async fn send(&self, bytes: &[u8]) -> io::Result<()> {
        self.carrier
            .send_to(&self.target, self.port, bytes)
            .await
            .map_err(io::Error::other)
    }
    async fn receive(&self, bytes: &mut [u8]) -> io::Result<usize> {
        self.carrier
            .recv_from(bytes)
            .await
            .map_err(io::Error::other)
    }
}
