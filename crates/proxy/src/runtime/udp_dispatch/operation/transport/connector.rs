use super::*;
use zero_transport::relay_connector::RelayStreamConnector;

struct RelayConnectorUdpOperation<TLeaf> {
    connector: RelayStreamConnector,
    prepared: PreparedTransportLeaf<TLeaf>,
}
pub(crate) fn prepare_transport_udp_relay_connector<'a, TLeaf>(
    connector: RelayStreamConnector,
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Box<dyn PreparedUdpFlowOperation + 'a>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf + Send + Sync + 'a,
{
    Box::new(RelayConnectorUdpOperation {
        connector,
        prepared,
    })
}
impl<TLeaf> PreparedUdpFlowOperation for RelayConnectorUdpOperation<TLeaf>
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
            let services = ctx.runtime_services();
            let stream = self
                .prepared
                .open_udp_relay_connector(services.upstream(), self.connector)
                .await
                .map_err(|error| FlowFailure {
                    stage: TLeaf::UDP_RELAY_CHAIN_STAGE,
                    error: error.into(),
                    upstream: None,
                })?;
            start_prepared_relay_stream(dispatch, services, session, payload, stream, self.prepared)
                .await
        })
    }
}

pub(super) async fn start_prepared_relay_stream<TLeaf>(
    dispatch: &mut UdpDispatch,
    services: UdpRuntimeServices,
    session: &Session,
    payload: &[u8],
    stream: crate::transport::TcpRelayStream,
    prepared: PreparedTransportLeaf<TLeaf>,
) -> Result<FlowStartResult, FlowFailure>
where
    TLeaf: ProxyRelayTwoStreamTransportLeaf,
{
    let mut context = dispatch.flow_start_context();
    let endpoint = prepared.endpoint();
    start_relay_managed_stream_packet(
        &mut context,
        ManagedStreamPacketStartBridge::relay(
            Some(services),
            endpoint.tag,
            session,
            ManagedStreamPacketRelay {
                carrier: RelayCarrier {
                    stream,
                    server: endpoint.server.to_owned(),
                    port: endpoint.port,
                }
                .into(),
                tls_server_name: None,
            },
            (endpoint.server, endpoint.port),
            prepared.relay_two_stream_udp_resume(),
            payload,
        ),
    )
    .await
}
