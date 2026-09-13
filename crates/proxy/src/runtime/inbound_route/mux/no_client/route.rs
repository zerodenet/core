use core::future::Future;

use tokio::io::{AsyncRead, AsyncWrite};
use tracing::warn;
use zero_core::{InboundMuxServer, InboundMuxStreamRoute, Session, StreamUdpResponder};
use zero_engine::EngineError;

use super::super::dispatch::dispatch_protocol_mux_route;
use super::super::model::{MuxRouteBridge, NoClientMuxRouteDefaults};
use crate::runtime::mux_session::{run_protocol_mux_session, MuxSessionLoop};
use crate::runtime::route_runtime::{InboundRouteRuntime, MuxSubstreamRuntime};
use crate::runtime::stream_udp::run_mapped_protocol_stream_udp_relay;
use crate::runtime::tcp_ingress::NoClientResponseInboundProtocol;
use crate::transport::TcpRelayStream;

pub(crate) async fn dispatch_no_client_mux_route<R, FTcp, FTcpFut, FUdp, FUdpFut>(
    route: R,
    runtime: InboundRouteRuntime,
    defaults: NoClientMuxRouteDefaults,
    spawn_tcp: FTcp,
    spawn_udp: FUdp,
) -> Result<(), EngineError>
where
    R: InboundMuxStreamRoute,
    R::TcpStream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    R::UdpRelay: zero_core::InboundStreamUdpRelay,
    <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Stream:
        AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
        StreamUdpResponder<TcpRelayStream>,
    R::MuxServer: InboundMuxServer<R::MuxReader>,
    R::MuxReader: Send,
    FTcp: FnMut(
            MuxSubstreamRuntime,
            Session,
            <R::MuxServer as InboundMuxServer<R::MuxReader>>::TcpRelay,
        ) -> FTcpFut
        + Send,
    FTcpFut: Future<Output = ()> + Send + 'static,
    FUdp: FnMut(
            MuxSubstreamRuntime,
            <R::MuxServer as InboundMuxServer<R::MuxReader>>::UdpRelay,
        ) -> FUdpFut
        + Send,
    FUdpFut: Future<Output = ()> + Send + 'static,
{
    dispatch_protocol_mux_route(
        route,
        MuxRouteBridge {
            runtime,
            protocol: NoClientResponseInboundProtocol,
            map_tcp_stream: TcpRelayStream::new,
            run_udp: move |runtime: InboundRouteRuntime,
                           session: Session,
                           relay: R::UdpRelay| async move {
                run_mapped_protocol_stream_udp_relay(
                    runtime.udp_runtime(),
                    &session,
                    relay,
                    runtime.inbound_tag(),
                    defaults.udp_protocol,
                    TcpRelayStream::new,
                    None,
                )
                .await
            },
            run_mux: move |runtime: MuxSubstreamRuntime,
                           reader: R::MuxReader,
                           mux_server: R::MuxServer| async move {
                let inbound_tag = runtime.inbound_tag().to_owned();
                match run_protocol_mux_session(
                    runtime,
                    reader,
                    mux_server,
                    MuxSessionLoop {
                        inbound_tag: inbound_tag.clone(),
                        protocol: defaults.mux_protocol,
                        panic_message: defaults.panic_message,
                        abort_on_end: defaults.abort_on_end,
                    },
                    spawn_tcp,
                    spawn_udp,
                )
                .await
                {
                    Ok(()) => Ok(()),
                    Err(error) => {
                        warn!(error = %error, "{}", defaults.read_error_log);
                        Ok(())
                    }
                }
            },
        },
    )
    .await
}

pub(crate) async fn dispatch_no_client_mux_route_with_defaults<R, FTcp, FTcpFut, FUdp, FUdpFut>(
    route: R,
    runtime: InboundRouteRuntime,
    defaults: NoClientMuxRouteDefaults,
    spawn_tcp: FTcp,
    spawn_udp: FUdp,
) -> Result<(), EngineError>
where
    R: InboundMuxStreamRoute,
    R::TcpStream: AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    R::UdpRelay: zero_core::InboundStreamUdpRelay,
    <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Stream:
        AsyncRead + AsyncWrite + Send + Sync + Unpin + 'static,
    <R::UdpRelay as zero_core::InboundStreamUdpRelay>::Responder:
        StreamUdpResponder<TcpRelayStream>,
    R::MuxServer: InboundMuxServer<R::MuxReader>,
    R::MuxReader: Send,
    FTcp: FnMut(
            MuxSubstreamRuntime,
            Session,
            <R::MuxServer as InboundMuxServer<R::MuxReader>>::TcpRelay,
        ) -> FTcpFut
        + Send,
    FTcpFut: Future<Output = ()> + Send + 'static,
    FUdp: FnMut(
            MuxSubstreamRuntime,
            <R::MuxServer as InboundMuxServer<R::MuxReader>>::UdpRelay,
        ) -> FUdpFut
        + Send,
    FUdpFut: Future<Output = ()> + Send + 'static,
{
    dispatch_no_client_mux_route(route, runtime, defaults, spawn_tcp, spawn_udp).await
}
