use zero_core::{
    InboundFallbackReplay, InboundMuxServer, InboundMuxStreamRoute, InboundMuxTcpRelay,
    InboundMuxUdpRelay,
};
use zero_engine::EngineError;

use super::result::{
    dispatch_recorded_protocol_mux_stream_request_result,
    dispatch_recorded_protocol_mux_tcp_request_result,
};
use crate::runtime::inbound_route::recorded::model::{
    RecordedProtocolMuxDispatch, RecordedProtocolMuxRouteDefaults,
};
use crate::runtime::mux_tcp::run_protocol_mux_tcp_task;
use crate::runtime::mux_udp::run_protocol_mux_udp_task_with_accept_log;
use crate::runtime::route_runtime::InboundRouteRuntime;
use crate::runtime::tcp_ingress::InboundProtocol;
use crate::runtime::PreparedInboundRouteAccept;
use crate::transport::{ClientStream, MeteredStream, RecordingStream};

pub(crate) async fn dispatch_recorded_protocol_mux_tcp_request_with_defaults<R, P, S, FR>(
    accept_result: Result<Option<PreparedInboundRouteAccept<R, FR>>, EngineError>,
    runtime: InboundRouteRuntime,
    protocol: P,
    defaults: RecordedProtocolMuxRouteDefaults,
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
    <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::TcpRelay: InboundMuxTcpRelay + 'static,
    <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::UdpRelay: InboundMuxUdpRelay + 'static,
{
    dispatch_recorded_protocol_mux_tcp_request_result(
        accept_result,
        RecordedProtocolMuxDispatch {
            runtime,
            protocol,
            defaults,
        },
        move |runtime, session, relay| {
            run_protocol_mux_tcp_task(runtime, session, relay, defaults.mux_protocol)
        },
        move |runtime, relay| {
            run_protocol_mux_udp_task_with_accept_log(
                runtime,
                relay,
                defaults.udp_protocol,
                defaults.udp_accept_log_message,
            )
        },
    )
    .await
}

pub(crate) async fn dispatch_recorded_protocol_mux_stream_request_with_defaults<R, P, S, FR>(
    accept_result: Result<PreparedInboundRouteAccept<R, FR>, EngineError>,
    runtime: InboundRouteRuntime,
    protocol: P,
    defaults: RecordedProtocolMuxRouteDefaults,
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
    <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::TcpRelay: InboundMuxTcpRelay + 'static,
    <R::MuxServer as InboundMuxServer<MeteredStream<S>>>::UdpRelay: InboundMuxUdpRelay + 'static,
{
    dispatch_recorded_protocol_mux_stream_request_result(
        accept_result,
        RecordedProtocolMuxDispatch {
            runtime,
            protocol,
            defaults,
        },
        move |runtime, session, relay| {
            run_protocol_mux_tcp_task(runtime, session, relay, defaults.mux_protocol)
        },
        move |runtime, relay| {
            run_protocol_mux_udp_task_with_accept_log(
                runtime,
                relay,
                defaults.udp_protocol,
                defaults.udp_accept_log_message,
            )
        },
    )
    .await
}
