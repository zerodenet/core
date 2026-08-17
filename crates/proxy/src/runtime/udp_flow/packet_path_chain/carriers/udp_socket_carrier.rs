use std::sync::Arc;

use async_trait::async_trait;
use zero_core::Address;
use zero_engine::EngineError;
use zero_traits::DatagramCodec;
use zero_transport::udp_packet_path::UdpSocketPacketPath;

use crate::protocol_registry::UdpNetworkServices;
use crate::runtime::udp_flow::packet_path::PacketPathCarrier;

pub(crate) async fn build(
    services: &UdpNetworkServices,
    server: &str,
    port: u16,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
    let endpoint = services
        .resolve_direct_address(
            &Address::Domain(server.to_owned()),
            port,
            "failed to resolve UDP socket packet-path carrier",
        )
        .await?;
    let sockets = services.outbound_datagram_socket_factory();
    let path = UdpSocketPacketPath::establish(endpoint, codec, &sockets).await?;
    Ok(Arc::new(PacketPathCarrierAdapter(path)))
}

struct PacketPathCarrierAdapter(UdpSocketPacketPath);

#[async_trait]
impl PacketPathCarrier for PacketPathCarrierAdapter {
    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.0
            .send_to(target, port, payload)
            .await
            .map_err(Into::into)
    }

    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError> {
        self.0.recv_from(buf).await.map_err(Into::into)
    }
}
