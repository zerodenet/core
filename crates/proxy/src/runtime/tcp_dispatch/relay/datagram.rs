//! Packet-path execution projected into a reusable carrier socket factory.
use crate::protocol_registry::{
    PacketPathExecutionServices, TcpExecutionServices, UdpNetworkServices,
};
use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;
use crate::runtime::udp_flow::packet_path::PacketPathCarrier;
use std::{io, sync::Arc};

pub(super) fn factory(
    services: &TcpExecutionServices,
    plan: PreparedDatagramRelayCarrier,
) -> zero_transport::OutboundDatagramSocketFactory {
    let network = UdpNetworkServices::from_tcp_execution(services);
    let execution = PacketPathExecutionServices::from_tcp_execution(services);
    network
        .outbound_datagram_socket_factory()
        .with_relay(Arc::new(move |peer| {
            let execution = execution.clone();
            let plan = plan.clone();
            Box::pin(async move {
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
