//! Atomically bound listener groups share reconciliation and shutdown ownership.
use super::PreparedInboundListenerOperation;
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};
use std::{future::Future, pin::Pin};
use zero_engine::EngineError;
pub(crate) struct InboundListenerGroupOperation(
    pub(crate) Vec<Box<dyn PreparedInboundListenerOperation>>,
);
impl PreparedInboundListenerOperation for InboundListenerGroupOperation {
    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        Box::pin(async move {
            let BoundInbound::Group(listeners) = bound else {
                return Err(EngineError::Io(std::io::Error::other(
                    "expected listener group",
                )));
            };
            if listeners.len() != self.0.len() {
                return Err(EngineError::Io(std::io::Error::other(
                    "listener group size mismatch",
                )));
            }
            use futures_util::{stream::FuturesUnordered, StreamExt};
            let mut operations: FuturesUnordered<_> = self
                .0
                .into_iter()
                .zip(listeners)
                .map(|(operation, listener)| {
                    operation.execute(runtime.clone(), listener, shutdown.clone())
                })
                .collect();
            while let Some(result) = operations.next().await {
                result?;
                if !*shutdown.borrow() {
                    return Err(EngineError::Io(std::io::Error::other(
                        "listener group member exited before shutdown",
                    )));
                }
            }
            Ok(())
        })
    }
}
