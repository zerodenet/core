use core::future::Future;

use zero_core::{InboundMuxServer, Session, StreamUdpResponder};
use zero_engine::EngineError;

use super::model::RecordedProtocolMuxRouteDefaults;
use crate::runtime::mux_session::{run_protocol_mux_session, MuxSessionLoop};
use crate::runtime::route_runtime::{InboundRouteRuntime, MuxSubstreamRuntime};
use crate::runtime::stream_udp::run_mapped_protocol_stream_udp_relay;
use crate::runtime::udp_ingress::UdpIngressRuntime;
use crate::transport::{ClientStream, MeteredStream, RecordingStream};

pub(crate) fn record_metered_inbound_traffic<S>(
    runtime: &UdpIngressRuntime,
    session_id: u64,
    client: &mut MeteredStream<S>,
) where
    S: ClientStream,
{
    runtime.record_session_inbound_traffic(session_id, client.drain_traffic());
}

pub(crate) fn record_metered_mux_inbound_traffic<S>(
    runtime: &MuxSubstreamRuntime,
    session_id: u64,
    client: &mut MeteredStream<S>,
) where
    S: ClientStream,
{
    runtime
        .udp_runtime()
        .record_session_inbound_traffic(session_id, client.drain_traffic());
}

pub(crate) async fn run_recorded_protocol_stream_udp_relay<S, R>(
    runtime: InboundRouteRuntime,
    session: Session,
    relay: R,
    protocol: &'static str,
) -> Result<(), EngineError>
where
    S: ClientStream + 'static,
    R: zero_core::InboundStreamUdpRelay<Stream = MeteredStream<RecordingStream<S>>>,
    R::Responder: StreamUdpResponder<MeteredStream<S>>,
{
    let session_id = session.id;
    let ingress = runtime.udp_runtime();
    let record_runtime = ingress.clone();
    run_mapped_protocol_stream_udp_relay(
        ingress,
        &session,
        relay,
        runtime.inbound_tag(),
        protocol,
        move |mut client| {
            record_runtime.record_session_inbound_traffic(session_id, client.drain_traffic());
            MeteredStream::new(client.into_unrecorded_inner())
        },
        Some(record_metered_inbound_traffic::<S>),
    )
    .await
}

pub(crate) async fn run_recorded_protocol_mux_session<S, M, FTcp, FTcpFut, FUdp, FUdpFut>(
    runtime: MuxSubstreamRuntime,
    mut reader: MeteredStream<RecordingStream<S>>,
    mux_server: M,
    defaults: RecordedProtocolMuxRouteDefaults,
    spawn_tcp: FTcp,
    spawn_udp: FUdp,
) -> Result<(), EngineError>
where
    S: ClientStream + 'static,
    M: InboundMuxServer<MeteredStream<S>>,
    FTcp: FnMut(MuxSubstreamRuntime, Session, M::TcpRelay) -> FTcpFut + Send,
    FTcpFut: Future<Output = ()> + Send + 'static,
    FUdp: FnMut(MuxSubstreamRuntime, M::UdpRelay) -> FUdpFut + Send,
    FUdpFut: Future<Output = ()> + Send + 'static,
{
    record_metered_mux_inbound_traffic(&runtime, 0, &mut reader);
    let client = MeteredStream::new(reader.into_unrecorded_inner());
    let inbound_tag = runtime.inbound_tag().to_owned();
    run_protocol_mux_session(
        runtime,
        client,
        mux_server,
        MuxSessionLoop {
            inbound_tag: &inbound_tag,
            protocol: defaults.mux_protocol,
            panic_message: defaults.panic_message,
            abort_on_end: defaults.abort_on_end,
        },
        spawn_tcp,
        spawn_udp,
    )
    .await
}
