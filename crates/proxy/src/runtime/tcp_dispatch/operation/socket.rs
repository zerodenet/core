use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_engine::EngineError;
use zero_transport::{RuntimeError, StreamTraffic};

use super::contract::{
    LazyTcpRelayCarrier, PreparedTcpConnectOperation, PreparedTcpRelayOperation,
};
use crate::protocol_registry::{TcpExecutionServices, UpstreamConnectServices};
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream};

#[async_trait::async_trait]
pub(crate) trait SocketTcpHandshake: Send + Sync {
    fn tag(&self) -> &str;

    fn server(&self) -> &str;

    fn port(&self) -> u16;

    fn connect_stage(&self) -> &'static str;

    async fn open_tcp_stream(
        &self,
        services: UpstreamConnectServices,
        session: &Session,
    ) -> Result<(TcpRelayStream, StreamTraffic), RuntimeError>;

    async fn open_tcp_relay_hop(
        &self,
        stream: TcpRelayStream,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError>;

    async fn open_tcp_relay_carrier(
        &self,
        carrier: LazyTcpRelayCarrier<'_>,
        session: &Session,
    ) -> Result<TcpRelayStream, EngineError> {
        self.open_tcp_relay_hop(carrier.open().await?, session)
            .await
            .map_err(Into::into)
    }
}

pub(crate) struct SocketTcpOperation<T> {
    pub(crate) handshake: T,
}

impl<T> PreparedTcpConnectOperation for SocketTcpOperation<T>
where
    T: SocketTcpHandshake + Send + Sync,
{
    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let handshake = &self.handshake;
            let endpoint = (handshake.server().to_owned(), handshake.port());
            let (stream, traffic) = handshake
                .open_tcp_stream(services.upstream_observed(handshake.tag()), session)
                .await
                .map_err(|error| TcpOutboundFailure {
                    stage: handshake.connect_stage(),
                    error: error.into(),
                    upstream_endpoint: Some(Box::new(endpoint.clone())),
                    network: None,
                })?;
            if !traffic.is_empty() {
                services.record_control_traffic(session.id, traffic);
            }
            Ok(EstablishedTcpOutbound::proxied(
                handshake.tag().to_owned(),
                endpoint.0,
                endpoint.1,
                stream,
            ))
        })
    }
}

impl<T> PreparedTcpRelayOperation for SocketTcpOperation<T>
where
    T: SocketTcpHandshake + Send + Sync,
{
    fn execute<'a>(
        &'a self,
        services: UpstreamConnectServices,
        stream: TcpRelayStream,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.handshake
                .open_tcp_relay_hop(services.observe_stream(stream), session)
                .await
                .map_err(Into::into)
        })
    }
    fn execute_lazy<'a>(
        &'a self,
        services: UpstreamConnectServices,
        carrier: LazyTcpRelayCarrier<'a>,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            self.handshake
                .open_tcp_relay_carrier(carrier.with_observer(services.observer()), session)
                .await
        })
    }
}
