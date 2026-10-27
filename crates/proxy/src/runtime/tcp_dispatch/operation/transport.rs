use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_engine::EngineError;

use super::contract::{PreparedTcpConnectOperation, PreparedTcpRelayOperation};
use crate::protocol_registry::TcpExecutionServices;
use crate::runtime::transport_leaf::{ProxyTransportLeaf, ProxyTransportTcpLeaf};
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream};

pub(crate) struct TransportLeafTcpOperation<TLeaf> {
    pub(crate) leaf: TLeaf,
}

impl<TLeaf> PreparedTcpConnectOperation for TransportLeafTcpOperation<TLeaf>
where
    TLeaf: ProxyTransportLeaf + ProxyTransportTcpLeaf + Send + Sync,
{
    fn retained_admission(&self) -> Result<(), EngineError> {
        self.leaf.retained_admission()
    }

    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let tag = self.leaf.tag().to_owned();
            let server = self.leaf.server().to_owned();
            let port = self.leaf.port();
            let (stream, traffic) = self
                .leaf
                .open_tcp_stream(services.upstream_observed(&tag), session)
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

impl<TLeaf> PreparedTcpRelayOperation for TransportLeafTcpOperation<TLeaf>
where
    TLeaf: ProxyTransportTcpLeaf + Send + Sync,
{
    fn retained_admission(&self) -> Result<(), EngineError> {
        self.leaf.retained_admission()
    }

    fn execute<'a>(
        &'a self,
        services: crate::protocol_registry::UpstreamConnectServices,
        stream: TcpRelayStream,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.leaf
                .open_tcp_relay_hop(services.clone(), services.observe_stream(stream), session)
                .await
                .map_err(Into::into)
        })
    }
    fn execute_lazy<'a>(
        &'a self,
        services: crate::protocol_registry::UpstreamConnectServices,
        carrier: super::contract::LazyTcpRelayCarrier<'a>,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.leaf
                .open_tcp_relay_carrier(
                    services.clone(),
                    carrier.with_observer(services.observer()),
                    session,
                )
                .await
        })
    }
}
