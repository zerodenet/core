//! Atomically bound listener groups share reconciliation and shutdown ownership.
use super::PreparedInboundListenerOperation;
use crate::{protocol_registry::BoundInbound, runtime::route_runtime::InboundListenerRuntime};
use std::{future::Future, pin::Pin};
use zero_engine::EngineError;
pub(crate) struct InboundListenerGroupOperation(
    pub(crate) Vec<Box<dyn PreparedInboundListenerOperation>>,
);
impl PreparedInboundListenerOperation for InboundListenerGroupOperation {
    fn internal_tasks(&self) -> usize {
        self.0
            .iter()
            .map(|operation| operation.internal_tasks())
            .sum()
    }
    fn install_resources(
        &mut self,
        context: &crate::runtime::inbound_owners::InboundInstallContext,
    ) -> Result<(), EngineError> {
        for operation in &mut self.0 {
            operation.install_resources(context)?;
        }
        Ok(())
    }

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
            let control = runtime.host_control().cloned();
            use futures_util::{stream::FuturesUnordered, StreamExt};
            let mut operations: FuturesUnordered<_> = self
                .0
                .into_iter()
                .zip(listeners)
                .map(|(operation, listener)| {
                    operation.execute(runtime.clone(), listener, shutdown.clone())
                })
                .collect();
            let mut failure = None;
            while let Some(result) = operations.next().await {
                let result = result.and_then(|()| {
                    if *shutdown.borrow() {
                        Ok(())
                    } else {
                        Err(EngineError::Io(std::io::Error::other(
                            "listener group member exited before shutdown",
                        )))
                    }
                });
                if let Err(error) = result {
                    if let Some(control) = &control {
                        control.request_stop();
                    }
                    failure.get_or_insert(error);
                }
                // Continue driving siblings to their real shutdown instead of
                // dropping their local connection owners on the first error.
            }
            failure.map_or(Ok(()), Err)
        })
    }
}
