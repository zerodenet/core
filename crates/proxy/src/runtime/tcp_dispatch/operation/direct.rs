use std::future::Future;
use std::pin::Pin;

use zero_core::Session;

use super::contract::PreparedTcpConnectOperation;
use crate::protocol_registry::TcpRuntimeServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure};

pub(crate) struct DirectTcpConnectOperation {
    pub(crate) tag: String,
}

impl PreparedTcpConnectOperation for DirectTcpConnectOperation {
    fn execute<'a>(
        self: Box<Self>,
        services: TcpRuntimeServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            execute_direct_tcp_operation(
                services,
                session,
                PreparedTcpOperation::Direct { tag: &self.tag },
            )
            .await
        })
    }
}

enum PreparedTcpOperation<'a> {
    Direct { tag: &'a str },
}

async fn execute_direct_tcp_operation(
    services: TcpRuntimeServices,
    session: &Session,
    operation: PreparedTcpOperation<'_>,
) -> Result<EstablishedTcpOutbound, TcpOutboundFailure> {
    let PreparedTcpOperation::Direct { tag } = operation;
    match services.connect_direct(session).await {
        Ok(connection) => Ok(EstablishedTcpOutbound::direct(
            tag,
            (connection.remote.ip().to_string(), connection.remote.port()),
            connection.socket.into(),
            connection.network,
        )),
        Err(failure) => Err(TcpOutboundFailure {
            stage: failure.stage,
            error: failure.error.into(),
            upstream_endpoint: None,
            network: Some(failure.network),
        }),
    }
}
