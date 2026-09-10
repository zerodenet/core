use std::sync::Arc;

use crate::runtime::udp_dispatch::operation::{
    ManagedDatagramStartPlan, ManagedDatagramUdpOperation, PreparedUdpFlowOperation,
};
use crate::runtime::udp_dispatch::packet_path_operation::PreparedDatagramRelayCarrier;
use crate::runtime::udp_flow::managed::datagram_manager::{
    managed_datagram_handler_box, ManagedDatagramConnectorFlow, ManagedDatagramResumeConnector,
};

pub(super) fn bind<'a>(
    plan: ::mieru::transport::MieruManagedUdpFlowPlan,
    carrier: PreparedDatagramRelayCarrier,
) -> Box<dyn PreparedUdpFlowOperation + 'a> {
    let (tag, server, port, protocol) = plan.into_parts();
    Box::new(ManagedDatagramUdpOperation {
        plan: ManagedDatagramStartPlan::from_parts((
            tag,
            server,
            port,
            MieruDatagramRelayResume {
                cache_namespace: format!("mieru-datagram-relay:{}:{port}", carrier.identity()),
                protocol,
                carrier: Arc::new(carrier),
            },
        )),
        needs_proxy: true,
    })
}

pub(crate) fn managed_datagram_handler(
) -> Box<dyn crate::runtime::udp_flow::managed::ManagedDatagramFlowHandler> {
    managed_datagram_handler_box::<MieruDatagramRelayResume>()
}

#[derive(Clone)]
struct MieruDatagramRelayResume {
    protocol: ::mieru::transport::MieruManagedUdpFlowResume,
    carrier: Arc<PreparedDatagramRelayCarrier>,
    cache_namespace: String,
}

impl std::fmt::Debug for MieruDatagramRelayResume {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MieruDatagramRelayResume")
            .field("carrier", &self.carrier)
            .field("cache_namespace", &self.cache_namespace)
            .finish_non_exhaustive()
    }
}

struct MieruPacketPathDatagramCarrier {
    carrier: Arc<dyn crate::runtime::udp_flow::packet_path::PacketPathCarrier>,
    target: zero_core::Address,
    port: u16,
}

#[async_trait::async_trait]
impl ::mieru::client::ClientDatagramCarrier for MieruPacketPathDatagramCarrier {
    async fn send(&self, packet: &[u8]) -> std::io::Result<()> {
        self.carrier
            .send_to(&self.target, self.port, packet)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))
    }

    async fn receive(&self, packet: &mut [u8]) -> std::io::Result<usize> {
        self.carrier
            .recv_from(packet)
            .await
            .map_err(|error| std::io::Error::other(error.to_string()))
    }
}

#[async_trait::async_trait]
impl ManagedDatagramResumeConnector for MieruDatagramRelayResume {
    type Connection = ::mieru::udp::MieruUdpFlowConnection;

    const ESTABLISH_STAGE: &'static str = "mieru_datagram_relay_establish";
    const MISMATCH_STAGE: &'static str = "udp_mieru_datagram_relay_resume";
    const MISMATCH_MESSAGE: &'static str = "expected Mieru datagram relay resume";

    fn connector_flow(
        &self,
        _endpoint: crate::runtime::path::OutboundEndpoint,
        session_id: u64,
    ) -> ManagedDatagramConnectorFlow {
        ManagedDatagramConnectorFlow::new(format!("{}:session={session_id}", self.cache_namespace))
    }

    async fn open_connection(
        self,
        services: crate::protocol_registry::UdpNetworkServices,
        endpoint: crate::runtime::path::OutboundEndpoint,
        initial_packet: crate::runtime::udp_flow::packet_path::UdpPacketRef<'_>,
    ) -> Result<Self::Connection, zero_engine::EngineError> {
        let generation = services.egress_generation();
        let relay_identity = format!("{}:egress={generation}", self.carrier.identity());
        let carrier_plan = self.carrier.clone();
        let carrier_services = services.clone();
        let target = zero_core::Address::Domain(endpoint.server.clone());
        let port = endpoint.port;
        let connection = self
            .protocol
            .open_datagram_relay_connection(generation, &relay_identity, move || {
                let carrier_plan = carrier_plan.clone();
                let services = carrier_services.clone();
                let target = target.clone();
                async move {
                    let carrier = carrier_plan.build(services).await.map_err(|error| {
                        zero_transport::RuntimeError::Io(std::io::Error::other(error.to_string()))
                    })?;
                    Ok::<
                        Arc<dyn ::mieru::client::ClientDatagramCarrier>,
                        zero_transport::RuntimeError,
                    >(Arc::new(MieruPacketPathDatagramCarrier {
                        carrier,
                        target,
                        port,
                    }))
                }
            })
            .await
            .map_err(zero_engine::EngineError::from)?;
        let sent = connection
            .send(
                initial_packet.target,
                initial_packet.port,
                initial_packet.payload,
            )
            .await
            .map_err(|error| {
                zero_engine::EngineError::Io(std::io::Error::other(error.to_string()))
            })?;
        if sent != initial_packet.payload.len() {
            return Err(zero_engine::EngineError::Io(std::io::Error::new(
                std::io::ErrorKind::WriteZero,
                "Mieru datagram relay accepted a partial initial packet",
            )));
        }
        Ok(connection)
    }
}
