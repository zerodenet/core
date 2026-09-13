use super::*;
use std::net::SocketAddr;
use zero_transport::{
    mkcp::{ListenerProfile, MkcpStream, PeerStreams, Settings},
    tls,
};

type RecordedAccept = zero_core::InboundRouteAccept<
    crate::inbound::VlessAcceptedClientRoute<
        zero_transport::MeteredStream<zero_transport::RecordingStream<TcpRelayStream>>,
    >,
    crate::inbound::VlessFallbackReplay<TcpRelayStream>,
>;
impl VlessInboundListenerRequest {
    pub fn with_mkcp(self, settings: Option<Settings>) -> Result<Self, RuntimeError> {
        self.with_mkcp_masks(settings, &[])
    }
    pub fn with_mkcp_masks(
        mut self,
        settings: Option<Settings>,
        masks: &[zero_transport::finalmask::udp::Mask],
    ) -> Result<Self, RuntimeError> {
        self.mkcp = settings
            .map(|settings| ListenerProfile::new_with_masks(settings, masks))
            .transpose()?;
        Ok(self)
    }
    pub fn uses_mkcp(&self) -> bool {
        self.mkcp.is_some()
    }
    pub fn accept_mkcp_peer(
        &self,
        socket: zero_platform_tokio::PacketSocket,
        peer: SocketAddr,
        packets: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) -> Result<PeerStreams, RuntimeError> {
        let profile = self
            .mkcp
            .as_ref()
            .ok_or_else(|| std::io::Error::other("missing mKCP listener profile"))?;
        Ok(profile.accept_peer(socket, peer, packets))
    }
    pub async fn accept_recorded_mkcp_route(
        self,
        stream: MkcpStream,
    ) -> Result<RecordedAccept, RuntimeError> {
        let mut metadata = VlessInboundStreamMetadata::from_stream(&stream);
        let stream = if let Some(acceptor) = self.transport.tls_acceptor.as_ref() {
            let stream = tls::accept_tls_handshake(acceptor, stream).await?;
            metadata.sni = stream.negotiated_server_name().map(str::to_owned);
            metadata.alpn = stream
                .negotiated_alpn()
                .map(|value| String::from_utf8_lossy(value).into_owned());
            TcpRelayStream::new(stream)
        } else {
            TcpRelayStream::new(stream)
        };
        self.accept_stream_route(stream, metadata, record_client_stream)
            .await
    }
}
