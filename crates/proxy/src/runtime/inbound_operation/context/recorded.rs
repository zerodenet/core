use super::model::InboundConnectionContext;

impl InboundConnectionContext {
    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) async fn dispatch_recorded_mux_tcp_route<R, P, S, FR>(
        self,
        accept_result: Result<
            Option<crate::runtime::PreparedInboundRouteAccept<R, FR>>,
            zero_engine::EngineError,
        >,
        protocol: P,
        defaults: crate::runtime::inbound_route::RecordedProtocolMuxRouteDefaults,
    ) -> Result<(), zero_engine::EngineError>
    where
        S: crate::transport::ClientStream + 'static,
        R: zero_core::InboundMuxStreamRoute<
            MuxReader = crate::transport::MeteredStream<crate::transport::RecordingStream<S>>,
        >,
        R::TcpStream: tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + zero_traits::AsyncSocket<Error = std::io::Error>
            + Send
            + Sync
            + Unpin
            + 'static,
        R::UdpRelay: zero_core::InboundStreamUdpRelay<
            Stream = crate::transport::MeteredStream<crate::transport::RecordingStream<S>>,
        >,
        R::MuxServer: zero_core::InboundMuxServer<crate::transport::MeteredStream<S>>,
        <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
            zero_core::StreamUdpResponder<crate::transport::MeteredStream<S>>,
        R::MuxReader: Send,
        P: crate::runtime::tcp_ingress::InboundProtocol<ClientStream = R::TcpStream> + 'static,
        FR: zero_core::InboundFallbackReplay + 'static,
        FR::Stream: crate::transport::ClientStream,
        <R::MuxServer as zero_core::InboundMuxServer<crate::transport::MeteredStream<S>>>::TcpRelay:
            zero_core::InboundMuxTcpRelay + 'static,
        <R::MuxServer as zero_core::InboundMuxServer<crate::transport::MeteredStream<S>>>::UdpRelay:
            zero_core::InboundMuxUdpRelay + 'static,
    {
        crate::runtime::inbound_route::dispatch_recorded_protocol_mux_tcp_request_with_defaults(
            accept_result,
            self.runtime,
            protocol,
            defaults,
        )
        .await
    }

    #[cfg(feature = "managed-stream-runtime")]
    pub(crate) async fn dispatch_recorded_mux_stream_route<R, P, S, FR>(
        self,
        accept_result: Result<
            crate::runtime::PreparedInboundRouteAccept<R, FR>,
            zero_engine::EngineError,
        >,
        protocol: P,
        defaults: crate::runtime::inbound_route::RecordedProtocolMuxRouteDefaults,
    ) -> Result<(), zero_engine::EngineError>
    where
        S: crate::transport::ClientStream + 'static,
        R: zero_core::InboundMuxStreamRoute<
            MuxReader = crate::transport::MeteredStream<crate::transport::RecordingStream<S>>,
        >,
        R::TcpStream: tokio::io::AsyncRead
            + tokio::io::AsyncWrite
            + zero_traits::AsyncSocket<Error = std::io::Error>
            + Send
            + Sync
            + Unpin
            + 'static,
        R::UdpRelay: zero_core::InboundStreamUdpRelay<
            Stream = crate::transport::MeteredStream<crate::transport::RecordingStream<S>>,
        >,
        R::MuxServer: zero_core::InboundMuxServer<crate::transport::MeteredStream<S>>,
        <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
            zero_core::StreamUdpResponder<crate::transport::MeteredStream<S>>,
        R::MuxReader: Send,
        P: crate::runtime::tcp_ingress::InboundProtocol<ClientStream = R::TcpStream> + 'static,
        FR: zero_core::InboundFallbackReplay + 'static,
        FR::Stream: crate::transport::ClientStream,
        <R::MuxServer as zero_core::InboundMuxServer<crate::transport::MeteredStream<S>>>::TcpRelay:
            zero_core::InboundMuxTcpRelay + 'static,
        <R::MuxServer as zero_core::InboundMuxServer<crate::transport::MeteredStream<S>>>::UdpRelay:
            zero_core::InboundMuxUdpRelay + 'static,
    {
        crate::runtime::inbound_route::dispatch_recorded_protocol_mux_stream_request_with_defaults(
            accept_result,
            self.runtime,
            protocol,
            defaults,
        )
        .await
    }
}
