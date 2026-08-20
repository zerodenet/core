use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_engine::EngineError;

use super::contract::{PreparedTcpConnectOperation, PreparedTcpRelayOperation};
use crate::protocol_registry::TcpRuntimeServices;
use crate::runtime::transport_leaf::{
    PreparedTransportLeaf, ProxyTransportLeaf, ProxyTransportTcpLeaf,
};
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream};

pub(crate) struct TransportLeafTcpConnectOperation<TLeaf> {
    pub(crate) prepared: PreparedTransportLeaf<TLeaf>,
}

impl<TLeaf> PreparedTcpConnectOperation for TransportLeafTcpConnectOperation<TLeaf>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportTcpLeaf + Send + Sync,
{
    fn execute<'a>(
        self: Box<Self>,
        services: TcpRuntimeServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let endpoint = self.prepared.endpoint();
            let tag = endpoint.tag.to_owned();
            let server = endpoint.server.to_owned();
            let port = endpoint.port;
            let (stream, traffic) = self
                .prepared
                .open_tcp_stream(services.upstream(), session)
                .await
                .map_err(|error| TcpOutboundFailure {
                    stage: TLeaf::TCP_CONNECT_STAGE,
                    error: error.into(),
                    upstream_endpoint: Some(Box::new((server.clone(), port))),
                    network: None,
                })?;
            if !traffic.is_empty() {
                services.record_control_traffic(session.id, traffic);
            }
            Ok(EstablishedTcpOutbound::proxied(tag, server, port, stream))
        })
    }
}

pub(crate) struct TransportLeafTcpRelayOperation<TLeaf> {
    pub(crate) prepared: PreparedTransportLeaf<TLeaf>,
}

impl<TLeaf> PreparedTcpRelayOperation for TransportLeafTcpRelayOperation<TLeaf>
where
    TLeaf: ProxyTransportTcpLeaf + Send + Sync,
{
    fn execute<'a>(
        self: Box<Self>,
        _services: TcpRuntimeServices,
        stream: TcpRelayStream,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.prepared
                .open_tcp_relay_hop(stream, session)
                .await
                .map_err(Into::into)
        })
    }
}
