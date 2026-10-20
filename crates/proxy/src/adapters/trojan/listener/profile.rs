//! Publish protocol-owned users only at the host's resource installation boundary.
use std::{future::Future, pin::Pin};

use ::trojan::{inbound::TrojanInboundProfileUpdate, transport::TrojanInboundListenerRequest};
use zero_engine::EngineError;

use crate::{
    protocol_registry::BoundInbound,
    runtime::{
        inbound_operation::{
            InboundConnectionContext, PreparedInboundListenerOperation, TcpInboundListenerOperation,
        },
        inbound_owners::InboundInstallContext,
        route_runtime::InboundListenerRuntime,
    },
};

struct StagedProfileOperation<D> {
    operation: TcpInboundListenerOperation<TrojanInboundListenerRequest, D>,
    update: Option<TrojanInboundProfileUpdate>,
}

pub(super) fn staged<D, Fut>(
    operation: TcpInboundListenerOperation<TrojanInboundListenerRequest, D>,
    update: TrojanInboundProfileUpdate,
) -> Box<dyn PreparedInboundListenerOperation>
where
    D: Fn(
            TrojanInboundListenerRequest,
            zero_platform_tokio::TokioSocket,
            InboundConnectionContext,
        ) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    Box::new(StagedProfileOperation {
        operation,
        update: Some(update),
    })
}

impl<D, Fut> PreparedInboundListenerOperation for StagedProfileOperation<D>
where
    D: Fn(
            TrojanInboundListenerRequest,
            zero_platform_tokio::TokioSocket,
            InboundConnectionContext,
        ) -> Fut
        + Clone
        + Send
        + Sync
        + 'static,
    Fut: Future<Output = Result<(), EngineError>> + Send + 'static,
{
    fn internal_tasks(&self) -> usize {
        self.operation.internal_tasks()
    }

    fn install_resources(&mut self, context: &InboundInstallContext) -> Result<(), EngineError> {
        self.operation.install_resources(context)?;
        if let Some(update) = self.update.take() {
            self.operation.request = self.operation.request.clone().with_profile(update.commit());
        }
        Ok(())
    }

    fn execute(
        self: Box<Self>,
        runtime: InboundListenerRuntime,
        bound: BoundInbound,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Pin<Box<dyn Future<Output = Result<(), EngineError>> + Send + 'static>> {
        if self.update.is_some() {
            return Box::pin(async {
                Err(EngineError::Io(std::io::Error::other(
                    "Trojan profile was not installed",
                )))
            });
        }
        Box::new(self.operation).execute(runtime, bound, shutdown)
    }
}

#[cfg(test)]
mod tests;
