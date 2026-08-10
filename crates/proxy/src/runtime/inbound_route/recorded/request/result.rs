use zero_core::{InboundFallbackReplay, InboundMuxServer, InboundMuxStreamRoute, Session};
use zero_engine::EngineError;

use crate::runtime::inbound_route::recorded::dispatch::{
    dispatch_optional_recorded_protocol_mux_route_accept_result,
    dispatch_recorded_protocol_mux_route_accept_result,
};
use crate::runtime::inbound_route::recorded::model::RecordedProtocolMuxDispatch;
use crate::runtime::route_runtime::MuxSubstreamRuntime;
use crate::runtime::tcp_ingress::InboundProtocol;
use crate::runtime::PreparedInboundRouteAccept;
use crate::transport::{ClientStream, MeteredStream, RecordingStream};

pub(crate) async fn dispatch_recorded_protocol_mux_tcp_request_result<
    R,
    P,
    S,
    FR,
    FTcp,
    FTcpFut,
    FUdp,
    FUdpFut,
>(
    accept_result: Result<Option<PreparedInboundRouteAccept<R, FR>>, EngineError>,
    request: RecordedProtocolMuxDispatch<P>,
    spawn_tcp: FTcp,
    spawn_udp: FUdp,
) -> Result<(), EngineError>
where
    S: ClientStream + 'static,
    R: InboundMuxStreamRoute<MuxReader = MeteredStream<RecordingStream<S>>>,
    R::TcpStream: tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + zero_traits::AsyncSocket<Error = std::io::Error>
        + Send
        + Sync
        + Unpin
        + 'static,
    R::UdpRelay: zero_core::InboundStreamUdpRelay<Stream = MeteredStream<RecordingStream<S>>>,
    R::MuxServer: InboundMuxServer<MeteredStream<S>>,
    <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
        zero_core::StreamUdpResponder<MeteredStream<S>>,
    R::MuxReader: Send,
    P: InboundProtocol<ClientStream = R::TcpStream> + 'static,
    FR: InboundFallbackReplay + 'static,
    FR::Stream: ClientStream,
    FTcp: FnMut(
            MuxSubstreamRuntime,
            Session,
            <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::TcpRelay,
        ) -> FTcpFut
        + Send,
    FTcpFut: core::future::Future<Output = ()> + Send + 'static,
    FUdp: FnMut(
            MuxSubstreamRuntime,
            <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::UdpRelay,
        ) -> FUdpFut
        + Send,
    FUdpFut: core::future::Future<Output = ()> + Send + 'static,
{
    dispatch_optional_recorded_protocol_mux_route_accept_result(
        accept_result?,
        request,
        spawn_tcp,
        spawn_udp,
    )
    .await
}

pub(crate) async fn dispatch_recorded_protocol_mux_stream_request_result<
    R,
    P,
    S,
    FR,
    FTcp,
    FTcpFut,
    FUdp,
    FUdpFut,
>(
    accept_result: Result<PreparedInboundRouteAccept<R, FR>, EngineError>,
    request: RecordedProtocolMuxDispatch<P>,
    spawn_tcp: FTcp,
    spawn_udp: FUdp,
) -> Result<(), EngineError>
where
    S: ClientStream + 'static,
    R: InboundMuxStreamRoute<MuxReader = MeteredStream<RecordingStream<S>>>,
    R::TcpStream: tokio::io::AsyncRead
        + tokio::io::AsyncWrite
        + zero_traits::AsyncSocket<Error = std::io::Error>
        + Send
        + Sync
        + Unpin
        + 'static,
    R::UdpRelay: zero_core::InboundStreamUdpRelay<Stream = MeteredStream<RecordingStream<S>>>,
    R::MuxServer: InboundMuxServer<MeteredStream<S>>,
    <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
        zero_core::StreamUdpResponder<MeteredStream<S>>,
    R::MuxReader: Send,
    P: InboundProtocol<ClientStream = R::TcpStream> + 'static,
    FR: InboundFallbackReplay + 'static,
    FR::Stream: ClientStream,
    FTcp: FnMut(
            MuxSubstreamRuntime,
            Session,
            <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::TcpRelay,
        ) -> FTcpFut
        + Send,
    FTcpFut: core::future::Future<Output = ()> + Send + 'static,
    FUdp: FnMut(
            MuxSubstreamRuntime,
            <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::UdpRelay,
        ) -> FUdpFut
        + Send,
    FUdpFut: core::future::Future<Output = ()> + Send + 'static,
{
    dispatch_recorded_protocol_mux_route_accept_result(
        accept_result?,
        request,
        spawn_tcp,
        spawn_udp,
    )
    .await
}
