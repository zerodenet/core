//! Runtime execution of a logical stream supplied by an admitted connection pool.
use super::PreparedTcpConnectOperation;
use crate::{
    protocol_registry::TcpExecutionServices,
    transport::{EstablishedTcpOutbound, TcpOutboundFailure, TcpRelayStream},
};
use std::{future::Future, pin::Pin, sync::Arc};
use zero_core::Session;

type OpenFuture = Pin<Box<dyn Future<Output = Result<TcpRelayStream, zero_core::Error>> + Send>>;

#[derive(Clone)]
pub(crate) struct LogicalTcpConnectOperation {
    pub(crate) tag: String,
    pub(crate) open: Arc<dyn Fn(Session) -> OpenFuture + Send + Sync>,
}
impl PreparedTcpConnectOperation for LogicalTcpConnectOperation {
    fn execute<'a>(
        &'a self,
        _services: TcpExecutionServices,
        session: &'a Session,
    ) -> Pin<Box<dyn Future<Output = Result<EstablishedTcpOutbound, TcpOutboundFailure>> + Send + 'a>>
    where
        Self: 'a,
    {
        Box::pin(async move {
            let stream =
                (self.open)(session.clone())
                    .await
                    .map_err(|error| TcpOutboundFailure {
                        stage: "logical_stream_connect",
                        error: error.into(),
                        upstream_endpoint: None,
                        network: None,
                    })?;
            Ok(EstablishedTcpOutbound::relay(
                &self.tag,
                None,
                Vec::new(),
                stream,
            ))
        })
    }
}
