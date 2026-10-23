//! The physical root is installed in the candidate's slot before acquisition.
use super::*;
use crate::runtime::udp_dispatch::packet_owner::{PacketCarrierDemand, PacketCarrierOwnerSlot};
use zero_transport::udp_packet_path::OwnedUdpSocketPacketPath;

pub(crate) async fn build(
    services: &UdpNetworkServices,
    owner: Arc<PacketCarrierOwnerSlot>,
    demand: PacketCarrierDemand,
    server: &str,
    port: u16,
    codec: Arc<dyn DatagramCodec<Address, Error = zero_core::Error>>,
) -> Result<Arc<dyn PacketPathCarrier>, EngineError> {
    demand.validate()?;
    if !demand.binding.prefix.is_empty() {
        return Err(
            crate::runtime::udp_dispatch::packet_path_operation::unsupported(
                "owned physical packet root cannot bypass a selected prefix",
            ),
        );
    }
    let attempt = owner.begin(&demand.binding)?;
    let opening = async {
        if services.egress_generation() != demand.binding.network_generation.0 {
            return Err(EngineError::Io(std::io::Error::other(
                "owned packet carrier network binding is stale",
            )));
        }
        let endpoint = services
            .resolve_node_address(
                &Address::Domain(server.to_owned()),
                port,
                "failed to resolve owned UDP socket packet-path carrier",
            )
            .await?;
        demand.validate()?;
        let wire = attempt.with_native(|native| {
            OwnedUdpSocketPacketPath::establish(
                endpoint,
                codec,
                &services.outbound_datagram_socket_factory(),
                native,
                &demand.binding.dial,
                demand.binding.network_generation.0,
            )
            .map_err(Into::into)
        })?;
        demand.validate()?;
        if attempt.stopped().is_cancelled() {
            return Err(EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "candidate packet carrier stopped after acquisition",
            )));
        }
        Ok(Arc::new(OwnedPacketPathCarrier {
            wire,
            owner: owner.clone(),
        }) as Arc<dyn PacketPathCarrier>)
    };
    let outcome = tokio::select! {
        biased;
        _ = attempt.stopped().cancelled() => Err(EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Interrupted, "candidate packet carrier stopped",
        ))),
        _ = demand.context.cancel.cancelled() => Err(EngineError::Io(std::io::Error::new(
            std::io::ErrorKind::Interrupted, "packet carrier demand cancelled",
        ))),
        result = zero_platform_tokio::contract::context::with_deadline(demand.context.deadline, opening) => {
            result.unwrap_or_else(|source| Err(EngineError::Io(std::io::Error::new(std::io::ErrorKind::TimedOut, source))))
        }
    };
    outcome.map_err(|error| attempt.retain_failure(error))
}
struct OwnedPacketPathCarrier {
    wire: OwnedUdpSocketPacketPath,
    owner: Arc<PacketCarrierOwnerSlot>,
}
#[async_trait]
impl PacketPathCarrier for OwnedPacketPathCarrier {
    fn native_owner(&self) -> Option<Arc<PacketCarrierOwnerSlot>> {
        Some(self.owner.clone())
    }

    async fn send_to(
        &self,
        target: &Address,
        port: u16,
        payload: &[u8],
    ) -> Result<(), EngineError> {
        self.wire
            .send_to(target, port, payload)
            .await
            .map_err(Into::into)
    }
    async fn recv_from(&self, buf: &mut [u8]) -> Result<usize, EngineError> {
        self.wire.recv_from(buf).await.map_err(Into::into)
    }
    async fn recv_from_with_source(
        &self,
        buf: &mut [u8],
    ) -> Result<(usize, Option<std::net::SocketAddr>), EngineError> {
        let (size, source, port) = self.wire.recv_from_with_source(buf).await?;
        Ok((
            size,
            crate::runtime::udp_flow::packet_path::packet_path_source(&source, port),
        ))
    }
}
