//! One persistent original JoinHandle and its bounded original close outcome.
use std::{future::Future, io, sync::Arc};
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinHandle},
};
use zero_platform_tokio::contract::{
    scope::{SpawnError, TaskClass, TaskJoinWitness, TaskRegistrar},
    ChannelError, Impact, Reason, Stage,
};

pub(super) struct DriverResult {
    pub(super) failure: Option<Arc<io::Error>>,
    pub(super) native: Option<Result<Arc<zero_tun::TunCloseReceipt>, Arc<io::Error>>>,
}
struct State {
    task: Option<JoinHandle<DriverResult>>,
    result: Option<Result<Arc<DriverResult>, Arc<JoinError>>>,
}
pub(super) struct DriverTask {
    state: Mutex<State>,
    hosted: Option<Hosted>,
}
struct Hosted {
    witness: TaskJoinWitness,
    result: Arc<std::sync::Mutex<Option<Arc<DriverResult>>>>,
}
impl DriverTask {
    pub(super) fn new(future: impl Future<Output = DriverResult> + Send + 'static) -> Self {
        // This is the original single driver, not an additional cleanup task.
        Self {
            state: Mutex::new(State {
                task: Some(tokio::spawn(future)),
                result: None,
            }),
            hosted: None,
        }
    }
    pub(super) fn scoped(
        tasks: &TaskRegistrar,
        future: impl Future<Output = DriverResult> + Send + 'static,
    ) -> Result<Self, SpawnError> {
        let result = Arc::new(std::sync::Mutex::new(None));
        let returned = result.clone();
        let (_, witness) = tasks.spawn_tracked(TaskClass::Critical, async move {
            let result = Arc::new(future.await);
            let failure = result.failure.clone();
            *returned
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
            match failure {
                Some(source) => {
                    Err(
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                            .with_source(SharedIo(source)),
                    )
                }
                None => Ok(()),
            }
        })?;
        Ok(Self {
            state: Mutex::new(State {
                task: None,
                result: None,
            }),
            hosted: Some(Hosted { witness, result }),
        })
    }
    pub(super) fn witness(&self) -> Option<TaskJoinWitness> {
        self.hosted.as_ref().map(|hosted| hosted.witness.clone())
    }
    pub(super) fn is_finished(&self) -> bool {
        if let Some(hosted) = &self.hosted {
            return hosted.witness.is_joined()
                || hosted
                    .result
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_some();
        }
        self.state.try_lock().map_or(true, |state| {
            state.result.is_some() || state.task.as_ref().is_none_or(JoinHandle::is_finished)
        })
    }
    async fn outcome(&self) -> Result<Arc<DriverResult>, io::Error> {
        if let Some(hosted) = &self.hosted {
            // Only the owning host consumes the original native exit. This
            // waiter observes that same receipt; cancellation keeps it in place.
            let joined = hosted.witness.wait().await;
            if let Err(source) = joined.result() {
                return Err(io::Error::other(SharedHosted(source)));
            }
            if let Some(result) = hosted
                .result
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
            {
                return Ok(result);
            }
            return Err(io::Error::other(
                "original scoped driver returned no retained outcome",
            ));
        }
        let mut state = self.state.lock().await;
        if state.result.is_none() {
            // Poll in place: cancelling a waiter never takes or drops this
            // exact original handle, nor a previously collected result.
            let result = state.task.as_mut().expect("unjoined driver").await;
            state.result = Some(result.map(Arc::new).map_err(Arc::new));
            state.task = None;
        }
        state
            .result
            .as_ref()
            .expect("driver consumed by actual join")
            .clone()
            .map_err(|error| io::Error::other(SharedJoin(error)))
    }
    pub(super) async fn join(&self) -> Result<Arc<DriverResult>, io::Error> {
        let result = self.outcome().await?;
        if let Some(error) = &result.failure {
            return Err(shared(error.clone()));
        }
        if let Some(Err(error)) = &result.native {
            return Err(shared(error.clone()));
        }
        Ok(result)
    }
    pub(super) async fn join_closed(&self) -> io::Result<()> {
        let result = self.join().await?;
        match &result.native {
            Some(Ok(_receipt)) => Ok(()),
            Some(Err(error)) => Err(shared(error.clone())),
            None => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "host packet device has no explicit native close receipt",
            )),
        }
    }
}
#[derive(Debug)]
struct SharedHosted(Arc<ChannelError>);
impl std::fmt::Display for SharedHosted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedHosted {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
pub(super) fn shared(error: Arc<io::Error>) -> io::Error {
    io::Error::new(error.kind(), SharedIo(error))
}
#[derive(Debug)]
pub(super) struct SharedIo(pub(super) Arc<io::Error>);
impl std::fmt::Display for SharedIo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedIo {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
#[derive(Debug)]
struct SharedJoin(Arc<JoinError>);
impl std::fmt::Display for SharedJoin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for SharedJoin {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

#[cfg(test)]
#[path = "tests/task.rs"]
mod tests;
