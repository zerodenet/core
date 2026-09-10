use std::future::Future;
use std::pin::Pin;

use super::PreparedUdpFlowOperation;
use crate::protocol_registry::{UdpAdapterContext, UdpRuntimeServices};
use crate::runtime::transport_leaf::{
    PreparedTransportLeaf, ProxyRelayTwoStreamTransportLeaf, ProxyTransportLeaf,
    ProxyTransportUdpLeaf,
};
use crate::runtime::udp_dispatch::{FlowFailure, FlowStartResult, UdpDispatch};
use crate::runtime::udp_flow::managed::bridge::{
    start_direct_managed_stream_packet, start_relay_managed_stream_packet,
    ManagedStreamPacketRelay, ManagedStreamPacketStartBridge,
};
use crate::transport::RelayCarrier;
use zero_core::Session;
use zero_engine::EngineError;

enum PreparedTransportUdpOperation<TLeaf> {
    Direct {
        prepared: PreparedTransportLeaf<TLeaf>,
    },
    RelayFinalHop {
        carrier: RelayCarrier,
        prepared: PreparedTransportLeaf<TLeaf>,
    },
}

struct TransportLeafUdpOperation<TLeaf> {
    operation: PreparedTransportUdpOperation<TLeaf>,
}

struct RelayTwoStreamUdpOperation<TLeaf> {
    post_carrier: RelayCarrier,
    get_carrier: RelayCarrier,
    prepared: PreparedTransportLeaf<TLeaf>,
}

pub(crate) fn prepare_transport_udp_direct<'a, TLeaf>(
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Box<dyn PreparedUdpFlowOperation + 'a>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf + Send + 'a,
{
    Box::new(TransportLeafUdpOperation::<TLeaf> {
        operation: PreparedTransportUdpOperation::Direct { prepared },
    })
}

pub(crate) fn prepare_transport_udp_relay_final_hop<'a, TLeaf>(
    carrier: RelayCarrier,
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Box<dyn PreparedUdpFlowOperation + 'a>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf + Send + 'a,
{
    Box::new(TransportLeafUdpOperation::<TLeaf> {
        operation: PreparedTransportUdpOperation::RelayFinalHop { carrier, prepared },
    })
}

pub(crate) fn prepare_transport_udp_relay_two_stream<'a, TLeaf>(
    post_carrier: RelayCarrier,
    get_carrier: RelayCarrier,
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Box<dyn PreparedUdpFlowOperation + 'a>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf + Send + Sync + 'a,
{
    Box::new(RelayTwoStreamUdpOperation::<TLeaf> {
        post_carrier,
        get_carrier,
        prepared,
    })
}

impl<TLeaf> PreparedUdpFlowOperation for TransportLeafUdpOperation<TLeaf>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf + Send,
{
    fn execute<'a>(
        self: Box<Self>,
        dispatch: &'a mut UdpDispatch,
        ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            execute_transport_udp_operation(
                dispatch,
                ctx.runtime_services(),
                session,
                payload,
                self.operation,
            )
            .await
        })
    }
}

impl<TLeaf> PreparedUdpFlowOperation for RelayTwoStreamUdpOperation<TLeaf>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf + Send + Sync,
{
    fn execute<'a>(
        self: Box<Self>,
        dispatch: &'a mut UdpDispatch,
        ctx: UdpAdapterContext<'a>,
        session: &'a Session,
        payload: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<FlowStartResult, FlowFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            execute_relay_two_stream_udp_operation(
                dispatch,
                ctx.runtime_services(),
                session,
                payload,
                self.post_carrier,
                self.get_carrier,
                self.prepared,
            )
            .await
        })
    }
}

async fn execute_relay_two_stream_udp_operation<TLeaf>(
    dispatch: &mut UdpDispatch,
    services: UdpRuntimeServices,
    session: &Session,
    payload: &[u8],
    post_carrier: RelayCarrier,
    get_carrier: RelayCarrier,
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Result<FlowStartResult, FlowFailure>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf,
{
    let mut context = dispatch.flow_start_context();
    let endpoint = prepared.endpoint();
    let resume = prepared.relay_two_stream_udp_resume();
    let paired_stream = prepared
        .open_relay_two_stream_udp_transport(post_carrier.stream, get_carrier.stream)
        .await
        .map_err(|error| FlowFailure {
            stage: TLeaf::UDP_RELAY_CHAIN_STAGE,
            error: error.into(),
            upstream: None,
        })?;

    start_relay_managed_stream_packet(
        &mut context,
        ManagedStreamPacketStartBridge::relay(
            Some(services),
            endpoint.tag,
            session,
            ManagedStreamPacketRelay {
                carrier: RelayCarrier {
                    stream: paired_stream,
                    server: endpoint.server.to_string(),
                    port: endpoint.port,
                }
                .into(),
                tls_server_name: None,
            },
            (endpoint.server, endpoint.port),
            resume,
            payload,
        ),
    )
    .await
}

async fn execute_transport_udp_operation<TLeaf>(
    dispatch: &mut UdpDispatch,
    services: UdpRuntimeServices,
    session: &Session,
    payload: &[u8],
    operation: PreparedTransportUdpOperation<TLeaf>,
) -> Result<FlowStartResult, FlowFailure>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportUdpLeaf,
{
    match operation {
        PreparedTransportUdpOperation::Direct { prepared } => {
            let mut context = dispatch.flow_start_context();
            let endpoint = prepared.endpoint();
            start_direct_managed_stream_packet(
                &mut context,
                ManagedStreamPacketStartBridge::direct(
                    services,
                    endpoint.tag,
                    session,
                    (endpoint.server, endpoint.port),
                    prepared.direct_udp_resume(),
                    payload,
                ),
            )
            .await
        }
        PreparedTransportUdpOperation::RelayFinalHop { carrier, prepared } => {
            let mut context = dispatch.flow_start_context();
            let endpoint = prepared.endpoint();
            prepared.validate_udp_relay_final_hop().map_err(|error| {
                udp_flow_failure(
                    "udp_relay_final_transport",
                    error.into(),
                    Some((endpoint.server, endpoint.port)),
                )
            })?;
            start_relay_managed_stream_packet(
                &mut context,
                ManagedStreamPacketStartBridge::relay(
                    Some(services),
                    endpoint.tag,
                    session,
                    ManagedStreamPacketRelay {
                        carrier: carrier.into(),
                        tls_server_name: None,
                    },
                    (endpoint.server, endpoint.port),
                    prepared.relay_final_hop_udp_resume(),
                    payload,
                ),
            )
            .await
        }
    }
}

fn udp_flow_failure(
    stage: &'static str,
    error: EngineError,
    upstream: Option<(&str, u16)>,
) -> FlowFailure {
    FlowFailure {
        stage,
        error,
        upstream: upstream.map(|(server, port)| (server.to_string(), port)),
    }
}
