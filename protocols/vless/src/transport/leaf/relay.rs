use super::*;

impl VlessOutboundLeaf {
    pub async fn open_tcp_relay_hop(
        &self,
        stream: TcpRelayStream,
        session: &Session,
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let protocol = self.protocol.clone();
        let mut transport = self.owned_transport_plan();
        transport.prepare_ech(ech_resolver.as_ref()).await?;
        let quic_requested = transport.uses_datagrams();
        protocol
            .open_tcp_relay_hop_with_transport(session, quic_requested, || {
                transport.open_relay(stream)
            })
            .await
            .map(TcpRelayStream::new)
    }

    pub async fn open_tcp_relay_connector(
        &self,
        connector: zero_transport::relay_connector::RelayStreamConnector,
        session: &Session,
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let protocol = self.protocol.clone();
        let mut transport = self.owned_transport_plan();
        transport.prepare_ech(ech_resolver.as_ref()).await?;
        protocol
            .open_tcp_relay_hop_with_transport(session, false, || {
                transport.open_relay_connector(connector, &self.relay_pools, &self.tag)
            })
            .await
            .map(TcpRelayStream::new)
    }

    pub fn udp_relay_uses_connector(&self) -> bool {
        true
    }
    pub async fn open_udp_relay_connector(
        &self,
        connector: zero_transport::relay_connector::RelayStreamConnector,
        ech_resolver: std::sync::Arc<dyn zero_transport::tls::ech::EchConfigResolver>,
    ) -> Result<TcpRelayStream, RuntimeError> {
        let mut transport = self.transport.clone();
        transport.prepare_ech(ech_resolver.as_ref()).await?;
        transport
            .open_relay_connector(connector, &self.relay_pools, &self.tag)
            .await
    }
}
