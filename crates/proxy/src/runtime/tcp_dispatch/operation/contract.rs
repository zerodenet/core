use std::future::Future;
use std::pin::Pin;

use zero_core::Session;
use zero_engine::EngineError;

use crate::protocol_registry::TcpExecutionServices;
use crate::transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream};

pub(crate) struct LazyTcpRelayCarrier<'a> {
    observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    open: Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>,
}

impl<'a> LazyTcpRelayCarrier<'a> {
    pub(crate) fn new(
        open: Pin<Box<dyn Future<Output = Result<TcpRelayStream, EngineError>> + Send + 'a>>,
    ) -> Self {
        Self {
            observer: None,
            open,
        }
    }

    pub(crate) fn with_observer(
        mut self,
        observer: Option<std::sync::Arc<dyn zero_traits::IoObserver>>,
    ) -> Self {
        self.observer = observer;
        self
    }
    pub(crate) async fn open(self) -> Result<TcpRelayStream, EngineError> {
        let stream = self.open.await?;
        Ok(match self.observer {
            Some(observer) => zero_transport::TcpRelayStream::new(
                zero_transport::observed::ObservedStream::new(stream, observer),
            ),
            None => stream,
        })
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
