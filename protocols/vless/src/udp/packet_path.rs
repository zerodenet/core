//! Packet-path preparation reuses protocol-owned UDP handshakes and carriers.
use crate::transport::{VlessManagedUdpFlowResume, VlessOutboundLeaf};
use std::future::Future;
use zero_core::{Address, Network, ProtocolType, Session};
use zero_platform_tokio::TokioSocket;
use zero_traits::{ProtocolOutboundLeaf, ProtocolRelayTwoStreamUdpFlowLeaf, ProtocolUdpFlowLeaf};
use zero_transport::{
    relay_connector::RelayStreamConnector, OutboundDatagramSocketFactory, RuntimeError,
};

#[derive(Clone)]
pub struct Plan {
    leaf: VlessOutboundLeaf,
    resume: VlessManagedUdpFlowResume,
    key: String,
    server: String,
    port: u16,
}
impl Plan {
    /// The owning adapter provides its serialized native configuration and base
    /// directory. Hash it here so credentials never appear in diagnostic keys.
    pub fn new(leaf: &VlessOutboundLeaf, native_identity: &str) -> Self {
        Self {
            leaf: leaf.clone(),
            resume: ProtocolUdpFlowLeaf::direct_udp_resume(leaf),
            key: format!(
                "vless-packet:{}",
                blake3::hash(native_identity.as_bytes()).to_hex()
            ),
            server: ProtocolOutboundLeaf::server(leaf).to_owned(),
            port: ProtocolOutboundLeaf::port(leaf),
        }
    }

    pub fn descriptor(&self) -> (String, String, u16) {
        (self.key.clone(), self.server.clone(), self.port)
    }

    pub async fn open<OpenSocket, OpenSocketFuture>(
        &self,
        target: Address,
        port: u16,
        open_socket: OpenSocket,
        sockets: OutboundDatagramSocketFactory,
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<super::VlessUdpFlowConnection, RuntimeError>
    where
        OpenSocket: Clone + Fn(&str, u16) -> OpenSocketFuture + Send + Sync + 'static,
        OpenSocketFuture: Future<Output = Result<TokioSocket, RuntimeError>> + Send + 'static,
    {
        // An independent XUDP global ID keeps inner associations distinct from
        // routed outer sessions and from other packet-path carrier instances.
        let session = packet_session(target, port);
        self.resume
            .open_direct_connection(&session, open_socket, sockets, ech_resolver)
            .await
    }

    /// Establish this VLESS hop through an already materialized, strictly
    /// shorter relay prefix. There is deliberately no direct fallback.
    pub async fn open_relay(
        &self,
        target: Address,
        port: u16,
        connector: RelayStreamConnector,
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<super::VlessUdpFlowConnection, RuntimeError> {
        let session = packet_session(target, port);
        let stream = self
            .leaf
            .open_udp_relay_connector(connector, ech_resolver.clone())
            .await?;
        ProtocolRelayTwoStreamUdpFlowLeaf::relay_two_stream_udp_resume(&self.leaf)
            .open_relay_connection(stream, &session, ech_resolver)
            .await
    }
}

fn packet_session(target: Address, port: u16) -> Session {
    Session::new(
        rand::random(),
        target,
        port,
        Network::Udp,
        ProtocolType::new("vless"),
    )
}
