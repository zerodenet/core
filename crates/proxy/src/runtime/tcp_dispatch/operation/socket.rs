use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_engine::EngineError;
use zero_transport::{RuntimeError, StreamTraffic};

use super::contract::{PreparedTcpConnectOperation, PreparedTcpRelayOperation};
use crate::protocol_registry::{TcpRuntimeServices, UpstreamConnectServices};
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
}

pub(crate) struct SocketTcpConnectOperation<T> {
    pub(crate) handshake: T,
}

impl<T> PreparedTcpConnectOperation for SocketTcpConnectOperation<T>
where
    T: SocketTcpHandshake + Send + Sync,
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
            execute_socket_tcp_connect_operation(
                services,
                session,
                PreparedSocketTcpOperation {
                    handshake: &self.handshake,
                },
            )
            .await
        })
    }
}

pub(crate) struct SocketTcpRelayOperation<T> {
    pub(crate) handshake: T,
}

impl<T> PreparedTcpRelayOperation for SocketTcpRelayOperation<T>
where
    T: SocketTcpHandshake + Send + Sync,
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
            execute_socket_tcp_relay_hop_operation(
                stream,
                session,
                PreparedSocketTcpOperation {
                    handshake: &self.handshake,
                },
            )
            .await
        })
    }
}

struct PreparedSocketTcpOperation<'leaf, T> {
    handshake: &'leaf T,
}

async fn execute_socket_tcp_connect_operation<T>(
    services: TcpRuntimeServices,
    session: &Session,
    operation: PreparedSocketTcpOperation<'_, T>,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure>
where
    T: SocketTcpHandshake,
{
    let handshake = operation.handshake;
    let endpoint = (handshake.server().to_owned(), handshake.port());
    let (stream, traffic) = handshake
        .open_tcp_stream(services.upstream(), session)
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
}

async fn execute_socket_tcp_relay_hop_operation<T>(
    stream: TcpRelayStream,
    session: &Session,
    operation: PreparedSocketTcpOperation<'_, T>,
) -> Result<TcpRelayStream, EngineError>
where
    T: SocketTcpHandshake,
{
    operation
        .handshake
        .open_tcp_relay_hop(stream, session)
        .await
        .map_err(Into::into)
}
