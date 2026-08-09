use core::future::Future;

use zero_core::{InboundMuxServer, InboundMuxStreamRoute, Session, StreamUdpResponder};
use zero_engine::EngineError;

use crate::runtime::inbound_route::recorded::helpers::{
    run_recorded_protocol_mux_session, run_recorded_protocol_stream_udp_relay,
};
use crate::runtime::inbound_route::recorded::model::RecordedProtocolMuxDispatch;
use crate::runtime::route_runtime::{InboundRouteRuntime, MuxSubstreamRuntime};
use crate::runtime::tcp_ingress::InboundProtocol;
use crate::transport::{ClientStream, MeteredStream, RecordingStream};

use crate::runtime::inbound_route::mux::{dispatch_protocol_mux_route, MuxRouteBridge};

pub(crate) async fn dispatch_recorded_protocol_mux_route<R, P, S, FTcp, FTcpFut, FUdp, FUdpFut>(
    route: R,
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
    let RecordedProtocolMuxDispatch {
        runtime,
        protocol,
        defaults,
    } = request;
    dispatch_protocol_mux_route(
        route,
        MuxRouteBridge {
            runtime,
            protocol,
            map_tcp_stream: core::convert::identity,
            run_udp: move |runtime: InboundRouteRuntime, session: Session, relay: R::UdpRelay| {
                run_recorded_protocol_stream_udp_relay::<S, _>(
                    runtime,
                    session,
                    relay,
                    defaults.udp_protocol,
                )
            },
            run_mux: move |runtime: MuxSubstreamRuntime,
                           reader: R::MuxReader,
                           mux_server: R::MuxServer| {
                run_recorded_protocol_mux_session(
                    runtime, reader, mux_server, defaults, spawn_tcp, spawn_udp,
                )
            },
        },
    )
    .await
}

pub(crate) async fn dispatch_recorded_protocol_mux_route_with_udp_logger<
    R,
    P,
    S,
    FTcp,
    FTcpFut,
    FUdp,
    FUdpFut,
>(
    route: R,
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
    dispatch_recorded_protocol_mux_route(route, request, spawn_tcp, spawn_udp).await
}
