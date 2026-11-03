//! HTTP carrier streams are authenticated independently by VLESS.
use super::*;
use zero_transport::split_http::XhttpIncoming;

pub(super) enum Incoming {
    Xhttp(XhttpIncoming),
    Grpc(zero_transport::grpc::GrpcIncoming),
    Scoped(zero_transport::quic::source::Streams),
}
impl Incoming {
    async fn accept(&self) -> Option<TcpRelayStream> {
        match self {
            Self::Scoped(incoming) => incoming.accept().await,
            Self::Xhttp(incoming) => incoming.accept().await.map(TcpRelayStream::new),
            Self::Grpc(incoming) => incoming.accept().await.map(TcpRelayStream::new),
        }
    }
    fn close(&self) {
        match self {
            Self::Scoped(incoming) => incoming.close(),
            Self::Xhttp(incoming) => incoming.close(),
            Self::Grpc(incoming) => incoming.close(),
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

    pub fn has_multiplexed_transport(&self) -> bool {
        self.transport.has_multiplexed_transport()
    }
}
