//! A declared TCP accept role, with the native original retained by ResourceAccess.
use super::super::{
    component::ResourceAccess,
    context::{with_deadline, AcceptContext},
    lifecycle::{CloseReport, ResourceLifecycle},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
use super::{InboundHandshake, InboundSource, PendingInbound};
use crate::{ControlledTcpListener, TokioSocket};
use std::{future::Future, sync::Arc};
use tokio::{sync::Mutex, time::Instant};
use zero_core::request::Destination;

struct State {
    listener: Mutex<Option<ControlledTcpListener>>,
    cancel: CancellationToken,
}
pub struct TcpSourceLifecycle {
    state: Arc<State>,
    resources: Arc<dyn ResourceAccess>,
    address: Destination,
    started: bool,
}
pub struct TcpSource<Make> {
    state: Arc<State>,
    make: Make,
}

/// Pure role/state construction. Only lifecycle start asks the real host for
/// the already-declared listener; no bind, protocol I/O or task starts here.
pub fn source<H, Make>(
    address: Destination,
    resources: Arc<dyn ResourceAccess>,
    cancel: CancellationToken,
    make: Make,
) -> (TcpSourceLifecycle, TcpSource<Make>)
where
    H: InboundHandshake,
    Make: Fn(TokioSocket) -> H + Send + Sync + 'static,
{
    let state = Arc::new(State {
        listener: Mutex::new(None),
        cancel: cancel.child_token(),
    });
    (
        TcpSourceLifecycle {
            state: state.clone(),
            resources,
            address,
            started: false,
        },
        TcpSource { state, make },
    )
}

impl ResourceLifecycle for TcpSourceLifecycle {
    async fn start(&mut self, deadline: Instant) -> Result<(), ChannelError> {
        if self.started || self.state.cancel.is_cancelled() {
            return Err(error(Stage::Prepare, Reason::Revoked));
        }
        // A failed/interrupted start never grants another acquisition attempt.
        self.started = true;
        let listener = tokio::select! { biased;
            _ = self.state.cancel.cancelled() => return Err(error(Stage::Prepare, Reason::Cancelled)),
            result = with_deadline(Some(deadline), self.resources.listen_tcp(self.address.clone())) => {
                result.map_err(|source| error(Stage::Prepare, Reason::Timeout).with_source(source))??
            }
        };
        *self.state.listener.lock().await = Some(listener);
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        self.state.cancel.cancel();
    }
    async fn shutdown(&mut self, deadline: Instant) -> Result<CloseReport, ChannelError> {
        self.state.cancel.cancel();
        let mut listener = with_deadline(Some(deadline), self.state.listener.lock())
            .await
            .map_err(|source| error(Stage::Close, Reason::Timeout).with_source(source))?;
        // Only the accept view is relinquished here. Host cleanup must still
        // close its original listener/accepted FDs and consume real Scope joins.
        let view = listener.take();
        drop(listener);
        drop(view);
        Ok(CloseReport::default())
    }
}
impl<H, Make> InboundSource for TcpSource<Make>
where
    H: InboundHandshake,
    Make: Fn(TokioSocket) -> H + Send + Sync + 'static,
{
    // The accepted entry is part of PendingInbound, not its protocol handshake.
    type Handshake = PendingInbound;
    fn next(
        &mut self,
        context: AcceptContext,
    ) -> impl Future<Output = Result<Option<Self::Handshake>, ChannelError>> + Send {
        async move {
            if context.cancel.is_cancelled() {
                return Err(error(Stage::Receive, Reason::Cancelled));
            }
            if context
                .deadline
                .is_some_and(|deadline| Instant::now() >= deadline)
            {
                return Err(error(Stage::Receive, Reason::Timeout));
            }
            let accept = async {
                let mut guard = self.state.listener.lock().await;
                let listener = guard
                    .as_mut()
                    .ok_or_else(|| error(Stage::Prepare, Reason::Revoked))?;
                let (accepted, _) = listener.accept_owned().await?;
                accepted.into_pending(&self.make).map(Some)
            };
            tokio::select! { biased;
                _ = context.cancel.cancelled() => Err(error(Stage::Receive, Reason::Cancelled)),
                _ = self.state.cancel.cancelled() => Ok(None),
                result = with_deadline(context.deadline, accept) => {
                    result.map_err(|source| error(Stage::Receive, Reason::Timeout).with_source(source))?
                }
            }
        }
    }
}

fn error(stage: Stage, reason: Reason) -> ChannelError {
    ChannelError::new(stage, reason, Impact::Component)
}
