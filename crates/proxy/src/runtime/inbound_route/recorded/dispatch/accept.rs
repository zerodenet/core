use core::future::Future;

use zero_core::{
    InboundFallbackReplay, InboundMuxServer, InboundMuxStreamRoute, InboundRouteAccept, Session,
    StreamUdpResponder,
};
use zero_engine::EngineError;

use super::route::dispatch_recorded_protocol_mux_route_with_udp_logger;
use crate::runtime::route_runtime::MuxSubstreamRuntime;
use crate::runtime::tcp_ingress::InboundProtocol;
use crate::transport::{ClientStream, MeteredStream, RecordingStream};

use crate::runtime::inbound_route::recorded::model::RecordedProtocolMuxDispatch;
use crate::runtime::{PreparedInboundFallback, PreparedInboundRouteAccept};

pub(crate) async fn dispatch_recorded_protocol_mux_route_accept_result<
    R,
    P,
    S,
    FR,
    FTcp,
    FTcpFut,
    FUdp,
    FUdpFut,
>(
    result: PreparedInboundRouteAccept<R, FR>,
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
        StreamUdpResponder<MeteredStream<S>>,
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
    FTcpFut: Future<Output = ()> + Send + 'static,
    FUdp: FnMut(
            MuxSubstreamRuntime,
            <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::UdpRelay,
        ) -> FUdpFut
        + Send,
    FUdpFut: Future<Output = ()> + Send + 'static,
{
    match result {
        InboundRouteAccept::Route(route) => {
            dispatch_recorded_protocol_mux_route_with_udp_logger(
                route, request, spawn_tcp, spawn_udp,
            )
            .await
        }
        InboundRouteAccept::Fallback(PreparedInboundFallback { target, replay }) => {
            request
                .runtime
                .relay_recorded_fallback_replay(target, replay)
                .await
        }
    }
}

pub(crate) async fn dispatch_optional_recorded_protocol_mux_route_accept_result<
    R,
    P,
    S,
    FR,
    FTcp,
    FTcpFut,
    FUdp,
    FUdpFut,
>(
    result: Option<PreparedInboundRouteAccept<R, FR>>,
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
        StreamUdpResponder<MeteredStream<S>>,
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
    FTcpFut: Future<Output = ()> + Send + 'static,
    FUdp: FnMut(
            MuxSubstreamRuntime,
            <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::UdpRelay,
        ) -> FUdpFut
        + Send,
    FUdpFut: Future<Output = ()> + Send + 'static,
{
    let Some(result) = result else {
        return Ok(());
    };
    dispatch_recorded_protocol_mux_route_accept_result(result, request, spawn_tcp, spawn_udp).await
}
