//! Concurrent requests register in the actual accepted entry when it exists.
//! Local tasks remain a compatibility owner, never native-close evidence.
use super::super::model::InboundRouteRuntime;
use futures_util::{stream::FuturesUnordered, StreamExt};
use tokio::task::JoinSet;
use zero_engine::EngineError;
use zero_platform_tokio::contract::inbound::PendingInbound;
use zero_platform_tokio::{
    contract::{lifecycle::TaskClass, BoxFuture, CancellationToken, ChannelError},
    AcceptedTasks,
};

pub(super) enum Requests {
    Local(JoinSet<()>),
    Owned {
        registrar: AcceptedTasks,
        joined: FuturesUnordered<BoxFuture<'static, ()>>,
    },
}
impl Requests {
    pub(super) fn new(registrar: Option<AcceptedTasks>) -> Self {
        match registrar {
            Some(registrar) => Self::Owned {
                registrar,
                joined: FuturesUnordered::new(),
            },
            None => Self::Local(JoinSet::new()),
        }
    }
    pub(super) fn len(&self) -> usize {
        match self {
            Self::Local(tasks) => tasks.len(),
            Self::Owned { joined, .. } => joined.len(),
        }
    }
    pub(super) fn spawn(
        &mut self,
        runtime: InboundRouteRuntime,
        pending: PendingInbound,
        cancel: CancellationToken,
    ) -> Result<(), ChannelError> {
        match self {
            Self::Local(tasks) => {
                tasks.spawn(run(runtime, pending, cancel));
            }
            Self::Owned { registrar, joined } => {
                let (_, witness) =
                    registrar.spawn(TaskClass::Auxiliary, move |tasks| async move {
                        let runtime = runtime.with_accepted_tasks(tasks)?;
                        run(runtime, pending, cancel).await;
                        Ok(())
                    })?;
                joined.push(Box::pin(async move {
                    let _original_join = witness.wait().await;
                }));
            }
        }
        Ok(())
    }
    pub(super) async fn join_next(&mut self) {
        match self {
            Self::Local(tasks) => {
                if let Some(Err(error)) = tasks.join_next().await {
                    if !error.is_cancelled() {
                        tracing::error!(%error, "standard inbound request panicked");
                    }
                }
            }
            Self::Owned { joined, .. } => {
                joined.next().await;
            }
        }
    }
}
async fn run(runtime: InboundRouteRuntime, pending: PendingInbound, cancel: CancellationToken) {
    tokio::select! { biased;
        _ = cancel.cancelled() => {},
        result = runtime.complete_request(pending, cancel.clone()) => report(result),
    }
}
pub(super) fn report(result: Result<(), EngineError>) {
    if let Err(error) = result {
        tracing::warn!(%error, "standard inbound request failed");
    }
}
