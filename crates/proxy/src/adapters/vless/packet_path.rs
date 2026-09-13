//! Native leaf projection; runtime owns target-flow caching and response tasks.
use crate::protocol_registry::{ClaimedUdpPacketPathLeaf, PacketPathExecutionServices};
use crate::runtime::udp_dispatch::packet_path_operation::{
    PacketPathCarrierFuture, PreparedUdpPacketPathOperation,
};
use crate::runtime::udp_flow::{
    managed::ManagedTupleUdpFlowConnection,
    packet_path::{self, PacketPathCarrierDescriptor},
};
use std::{path::Path, sync::Arc};

pub(super) fn claim<'a, F>(
    identity: String,
    prepare: F,
) -> Box<dyn ClaimedUdpPacketPathLeaf<'a> + 'a>
where
    F: Fn(Option<&Path>) -> Result<::vless::transport::VlessOutboundLeaf, zero_core::Error>
        + Send
        + Sync
        + 'a,
{
    Box::new(Claim { identity, prepare })
}
struct Claim<F> {
    identity: String,
    prepare: F,
}
impl<'a, F> ClaimedUdpPacketPathLeaf<'a> for Claim<F>
where
    F: Fn(Option<&Path>) -> Result<::vless::transport::VlessOutboundLeaf, zero_core::Error>
        + Send
        + Sync
        + 'a,
{
    fn prepare_udp_packet_path(
        &self,
        source_dir: Option<&Path>,
    ) -> Option<Box<dyn PreparedUdpPacketPathOperation>> {
        let leaf = match (self.prepare)(source_dir) {
            Ok(leaf) => leaf,
            Err(error) => {
                tracing::debug!(%error, "packet-path leaf preparation failed");
                return None;
            }
        };
        let identity = serde_json::to_string(&(&self.identity, source_dir)).ok()?;
        Some(Box::new(Operation(::vless::udp::packet_path::Plan::new(
            &leaf, &identity,
        ))))
    }
}
struct Operation(::vless::udp::packet_path::Plan);
impl PreparedUdpPacketPathOperation for Operation {
    fn carrier_descriptor(&self) -> Option<PacketPathCarrierDescriptor> {
        let (cache_key, server, port) = self.0.descriptor();
        Some(PacketPathCarrierDescriptor {
            cache_key,
            server,
            port,
        })
    }
    fn supports_associated_relay_carrier(&self) -> bool {
        true
    }

    fn build_carrier<'a>(
        &'a self,
        services: PacketPathExecutionServices,
    ) -> PacketPathCarrierFuture<'a> {
        let plan = self.0.clone();
        let ech_resolver = services.ech_resolver();
        let services = services.network();
        Box::pin(async move {
            let network = services.clone();
            let open: packet_path::tuple_flow::Open = Arc::new(move |target, port| {
                let plan = plan.clone();
                let network = network.clone();
                let ech_resolver = ech_resolver.clone();
                Box::pin(async move {
                    let sockets = network.outbound_datagram_socket_factory();
                    let connection = plan
                        .open(
                            target,
                            port,
                            move |server, port| {
                                let server = server.to_owned();
                                let network = network.clone();
                                async move { network.connect_upstream(&server, port).await }
                            },
                            sockets,
                            ech_resolver,
                        )
                        .await
                        .map_err(zero_engine::EngineError::from)?;
                    Ok(Box::new(connection) as Box<dyn ManagedTupleUdpFlowConnection>)
                })
            });
            Ok(packet_path::tuple_flow::carrier(services, open))
        })
    }

    fn build_associated_relay_carrier<'a>(
        &'a self,
        services: PacketPathExecutionServices,
        carrier: crate::runtime::tcp_dispatch::operation::LazyTcpRelayCarrier<'a>,
    ) -> PacketPathCarrierFuture<'a> {
        let Some(connector) = carrier.connector() else {
            return Box::pin(async {
                Err(zero_engine::EngineError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "VLESS associated packet path requires a relay connector",
                )))
            });
        };
        let plan = self.0.clone();
        let ech_resolver = services.ech_resolver();
        let services = services.network();
        Box::pin(async move {
            let network = services.clone();
            let open: packet_path::tuple_flow::Open = Arc::new(move |target, port| {
                let plan = plan.clone();
                let connector = connector.clone();
                let ech_resolver = ech_resolver.clone();
                Box::pin(async move {
                    let connection = plan
                        .open_relay(target, port, connector, ech_resolver)
                        .await
                        .map_err(zero_engine::EngineError::from)?;
                    Ok(Box::new(connection) as Box<dyn ManagedTupleUdpFlowConnection>)
                })
            });
            Ok(packet_path::tuple_flow::carrier(network, open))
        })
    }
}
