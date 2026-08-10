use super::model::InboundConnectionContext;

impl InboundConnectionContext {
    #[cfg(feature = "managed-stream-runtime")]
    #[allow(dead_code)]
    pub(crate) async fn dispatch_no_client_stream_route<R>(
        self,
        route: R,
        udp_protocol: &'static str,
    ) -> Result<(), zero_engine::EngineError>
    where
        R: zero_core::InboundStreamRoute,
        R::TcpStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
        R::UdpRelay: zero_core::InboundStreamUdpRelay,
        <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Stream:
            tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
        <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
            zero_core::StreamUdpResponder<crate::transport::TcpRelayStream>,
    {
        crate::runtime::inbound_route::dispatch_no_client_stream_route(
            route,
            self.runtime,
            udp_protocol,
        )
        .await
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) async fn dispatch_no_client_mux_route<R>(
        self,
        route: R,
        defaults: crate::runtime::inbound_route::NoClientMuxRouteDefaults,
    ) -> Result<(), zero_engine::EngineError>
    where
        R: zero_core::InboundMuxStreamRoute,
        R::TcpStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
        R::UdpRelay: zero_core::InboundStreamUdpRelay,
        <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Stream:
            tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + Sync + Unpin + 'static,
        <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
            zero_core::StreamUdpResponder<crate::transport::TcpRelayStream>,
        R::MuxServer: zero_core::InboundMuxServer<R::MuxReader>,
        R::MuxReader: Send,
        <R::MuxServer as zero_core::InboundMuxServer<R::MuxReader>>::TcpRelay:
            zero_core::InboundMuxTcpRelay + 'static,
        <R::MuxServer as zero_core::InboundMuxServer<R::MuxReader>>::UdpRelay:
            zero_core::InboundMuxUdpRelay + 'static,
    {
        crate::runtime::inbound_route::dispatch_no_client_mux_route_request_with_defaults(
            route,
            self.runtime,
            defaults,
        )
        .await
    }
}
