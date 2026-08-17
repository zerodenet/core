use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_transport::{RuntimeError, TcpRelayStream};

use super::contract::PreparedTcpConnectOperation;
use crate::protocol_registry::TcpRuntimeServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

#[async_trait::async_trait]
pub(crate) trait SessionTcpHandshake: Send + Sync {
    fn tag(&self) -> &str;

    fn server(&self) -> &str;

    fn port(&self) -> u16;

    fn connect_stage(&self) -> &'static str;

    async fn open_tcp_stream(
        &self,
        services: crate::protocol_registry::UpstreamConnectServices,
        session: &Session,
    ) -> Result<TcpRelayStream, RuntimeError>;
}

pub(crate) struct SessionTcpConnectOperation<T> {
    pub(crate) handshake: T,
}

impl<T> PreparedTcpConnectOperation for SessionTcpConnectOperation<T>
where
    T: SessionTcpHandshake + Send + Sync,
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
            execute_session_tcp_connect_operation(
                services,
                session,
                PreparedSessionTcpOperation {
                    handshake: &self.handshake,
                },
            )
            .await
        })
    }
}

struct PreparedSessionTcpOperation<'leaf, T> {
    handshake: &'leaf T,
}

async fn execute_session_tcp_connect_operation<T>(
    services: TcpRuntimeServices,
    session: &Session,
    operation: PreparedSessionTcpOperation<'_, T>,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure>
where
    T: SessionTcpHandshake,
{
    let handshake = operation.handshake;
    let endpoint = (handshake.server().to_owned(), handshake.port());
    let stream = handshake
        .open_tcp_stream(services.upstream(), session)
        .await
        .map_err(|error| TcpOutboundFailure {
            stage: handshake.connect_stage(),
            error: error.into(),
            upstream_endpoint: Some(endpoint.clone()),
        })?;
    Ok(EstablishedTcpOutbound::proxied(
        handshake.tag().to_owned(),
        endpoint.0,
        endpoint.1,
        stream,
    ))
}
