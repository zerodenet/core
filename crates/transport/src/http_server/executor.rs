use super::*;
#[derive(Clone)]
pub(super) enum Executor {
    Legacy,
    Owned {
        caller: AcceptedCaller,
        stop: CancellationToken,
    },
}
pub(super) enum TaskStop {
    Legacy(tokio::task::AbortHandle),
    Owned(CancellationToken),
}
impl TaskStop {
    pub(super) fn cancel(&self) {
        match self {
            Self::Legacy(task) => task.abort(),
            Self::Owned(stop) => stop.cancel(),
        }
    }
}
impl Executor {
    pub(super) fn spawn(
        &self,
        future: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> io::Result<TaskStop> {
        match self {
            Self::Legacy => Ok(TaskStop::Legacy(tokio::spawn(future).abort_handle())),
            Self::Owned { caller, stop } => {
                let request = stop.child_token();
                let cancel = request.clone();
                caller
                    .spawn(TaskClass::Auxiliary, move |_| async move {
                        tokio::select! {biased;_=cancel.cancelled()=>{},_=future=>{}}
                        Ok(())
                    })
                    .map_err(io::Error::other)?;
                Ok(TaskStop::Owned(request))
            }
        }
    }
}
impl<F> hyper::rt::Executor<F> for Executor
where
    F: std::future::Future + Send + 'static,
    F::Output: Send,
{
    fn execute(&self, future: F) {
        // Hyper internal tasks are also charged to this accepted origin.
        // Their stop token is retained by the connection, not a detached handle.
        if let Err(error) = self.spawn(async move {
            let _ = future.await;
        }) {
            if let Self::Owned { stop, .. } = self {
                stop.cancel();
            }
            tracing::debug!(%error,"HTTP task admission failed");
        }
    }
}
