//! HTTP carrier streams are authenticated independently by VLESS.
use super::*;
use zero_transport::split_http::XhttpIncoming;

pub(super) enum Incoming {
    Xhttp(XhttpIncoming),
    Grpc(zero_transport::grpc::GrpcIncoming),
    Hysteria(zero_transport::hysteria::Incoming),
}
impl Incoming {
    async fn accept(&self) -> Option<TcpRelayStream> {
        match self {
            Self::Xhttp(incoming) => incoming.accept().await.map(TcpRelayStream::new),
            Self::Grpc(incoming) => incoming.accept().await.map(TcpRelayStream::new),
            Self::Hysteria(incoming) => incoming.accept().await.map(TcpRelayStream::new),
        }
    }
    fn close(&self) {
        match self {
            Self::Xhttp(incoming) => incoming.close(),
            Self::Grpc(incoming) => incoming.close(),
            Self::Hysteria(incoming) => incoming.close(),
        }
    }
}

pub struct VlessInboundTransportStreams {
    pub(super) incoming: Incoming,
    pub(super) metadata: VlessInboundStreamMetadata,
}
impl zero_core::InboundTransportMultiplexer for VlessInboundTransportStreams {
    type Stream = (TcpRelayStream, VlessInboundStreamMetadata);
    async fn accept_stream(&self) -> Option<Self::Stream> {
        self.incoming
            .accept()
            .await
            .map(|stream| (stream, self.metadata.clone()))
    }
    fn close(&self) {
        self.incoming.close();
    }
}
impl VlessInboundListenerRequest {
    pub fn with_hysteria(mut self, profile: Option<zero_transport::hysteria::Profile>) -> Self {
        self.hysteria = profile;
        self
    }
    pub fn uses_hysteria(&self) -> bool {
        self.hysteria.is_some()
    }

    pub fn accept_http3_streams(
        &self,
        connection: zero_transport::quic::QuicConnection,
        local: std::net::SocketAddr,
    ) -> Result<VlessInboundTransportStreams, RuntimeError> {
        if let Some(profile) = &self.hysteria {
            let mut metadata = VlessInboundStreamMetadata::from_quic(&connection);
            metadata.destination = Some(local);
            return Ok(VlessInboundTransportStreams {
                incoming: Incoming::Hysteria(zero_transport::hysteria::accept_connection(
                    connection, profile,
                )),
                metadata,
            });
        }
        let profile = self
            .transport
            .split_http
            .as_ref()
            .ok_or_else(|| std::io::Error::other("HTTP/3 requires an XHTTP profile"))?;
        let registry = self
            .transport
            .split_http_registry
            .as_ref()
            .ok_or_else(|| std::io::Error::other("missing XHTTP listener registry"))?;
        let mut metadata = VlessInboundStreamMetadata::from_quic(&connection);
        metadata.destination = Some(local);
        Ok(VlessInboundTransportStreams {
            incoming: Incoming::Xhttp(zero_transport::split_http::accept_xhttp_h3_connection(
                connection, profile, registry,
            )),
            metadata,
        })
    }
    pub fn has_multiplexed_transport(&self) -> bool {
        self.transport.has_multiplexed_transport()
    }
    pub async fn accept_transport_streams(
        &self,
        socket: TokioSocket,
    ) -> Result<
        zero_core::InboundRouteAccept<VlessInboundTransportStreams, VlessTcpFallbackReplay>,
        RuntimeError,
    > {
        self.transport
            .clone()
            .accept_transport_streams(socket)
            .await
    }
    pub async fn accept_recorded_transport_stream(
        self,
        incoming: (TcpRelayStream, VlessInboundStreamMetadata),
    ) -> Result<
        zero_core::InboundRouteAccept<
            crate::inbound::VlessAcceptedClientRoute<
                zero_transport::MeteredStream<zero_transport::RecordingStream<TcpRelayStream>>,
            >,
            crate::inbound::VlessFallbackReplay<TcpRelayStream>,
        >,
        RuntimeError,
    > {
        self.accept_stream_route(incoming.0, incoming.1, record_client_stream)
            .await
    }
}
