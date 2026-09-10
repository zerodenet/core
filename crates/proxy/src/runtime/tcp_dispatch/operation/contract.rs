use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_engine::EngineError;

use crate::protocol_registry::TcpRuntimeServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream};

pub(crate) struct LazyTcpRelayCarrier<'a> {
    identity: String,
    generation: u64,
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
            open,
        }
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

pub(crate) trait PreparedTcpConnectOperation: Send {
    fn execute<'a>(
        self: Box<Self>,
        services: TcpRuntimeServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a;
}

pub(crate) trait PreparedTcpRelayOperation: Send {
    fn execute<'a>(
        self: Box<Self>,
        stream: TcpRelayStream,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a;

    fn execute_lazy<'a>(
        self: Box<Self>,
        carrier: LazyTcpRelayCarrier<'a>,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let stream = carrier.open().await?;
            self.execute(stream, session).await
        })
    }
}
