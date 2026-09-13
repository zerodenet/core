use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_engine::EngineError;

use crate::protocol_registry::TcpExecutionServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream};

pub(crate) struct LazyTcpRelayCarrier<'a> {
    identity: String,
    generation: u64,
    connector: Option<zero_transport::relay_connector::RelayStreamConnector>,
    open: Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>,
}

impl<'a> LazyTcpRelayCarrier<'a> {
    pub(crate) fn new(
        identity: String,
        generation: u64,
        open: Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>,
    ) -> Self {
        Self {
            identity,
            generation,
            connector: None,
            open,
        }
    }

    pub(crate) fn with_connector(
        mut self,
        connector: zero_transport::relay_connector::RelayStreamConnector,
    ) -> Self {
        self.connector = Some(connector);
        self
    }
    pub(crate) fn connector(
        &self,
    ) -> Option<zero_transport::relay_connector::RelayStreamConnector> {
        self.connector.clone()
    }

    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }

    pub(crate) fn generation(&self) -> u64 {
        self.generation
    }

    pub(crate) async fn open(self) -> Result<TcpRelayStream, EngineError> {
        self.open.await
    }
}

pub(crate) trait PreparedTcpConnectOperation: Send + Sync {
    fn execute<'a>(
        &'a self,
        services: TcpExecutionServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a;
}

pub(crate) trait PreparedTcpRelayOperation: Send + Sync {
    fn execute<'a>(
        &'a self,
        services: crate::protocol_registry::UpstreamConnectServices,
        stream: TcpRelayStream,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a;

    fn execute_lazy<'a>(
        &'a self,
        services: crate::protocol_registry::UpstreamConnectServices,
        carrier: LazyTcpRelayCarrier<'a>,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let stream = carrier.open().await?;
            self.execute(services, stream, session).await
        })
    }
}
