use super::{registry::Requests, CURRENT};
use std::{future::Future, sync::Arc};
use tokio_util::sync::CancellationToken;
use zero_platform_tokio::{
    contract::{scope::TaskClass, ChannelError, Impact, Reason, Stage},
    AcceptedTasks,
};
use zero_runtime::configuration::ControlConnectionBudget;

#[derive(Clone)]
pub(crate) struct StreamExecutor {
    pub(crate) tasks: AcceptedTasks,
    pub(crate) stop: CancellationToken,
    pub(crate) requests: Arc<Requests>,
    pub(crate) connection: ControlConnectionBudget,
}
impl<F> hyper::rt::Executor<F> for StreamExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        let original = match self.requests.reserve(&self.connection) {
            Ok(original) => original,
            Err(error) => {
                tracing::debug!(%error, "new gRPC stream exceeds its actual root or connection budget");
                return;
            }
        };
        let stop = self.stop.clone();
        let held = original.clone();
        match self
            .tasks
            .spawn(TaskClass::Auxiliary, move |tasks| async move {
                let (_, witness) = tasks.current_task().ok_or_else(|| {
                    ChannelError::new(Stage::Prepare, Reason::Internal, Impact::Component)
                })?;
                if !held.bind(witness) {
                    return Err(ChannelError::new(
                        Stage::Prepare,
                        Reason::StalePlan,
                        Impact::Component,
                    ));
                }
                CURRENT
                    .scope(held.clone(), async move {
                        let work = super::super::native::catch_work(async {
                            tokio::select! { _ = stop.cancelled() => {}, _ = future => {} }
                        })
                        .await;
                        // Keep the same accepted stream task and original
                        // command future through its real final ACK, even if
                        // Hyper has dropped the disconnected response handler.
                        held.command.drain().await;
                        if let Err(payload) = work {
                            std::panic::resume_unwind(payload);
                        }
                    })
                    .await;
                Ok(())
            }) {
            Ok((_, witness)) => {
                // The same actual context can run on another worker already;
                // both paths bind this exact registration, never a task number.
                assert!(original.bind(witness));
            }
            Err(error) => {
                self.requests.rejected(&original);
                tracing::warn!(%error, "new gRPC H2 stream task rejected by its original entry");
            }
        }
    }
}
