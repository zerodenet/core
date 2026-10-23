//! One persistent original JoinHandle and its bounded original close outcome.
use std::{future::Future, io, sync::Arc};
use tokio::{
    sync::Mutex,
    task::{JoinError, JoinHandle},
};

pub(super) struct DriverResult {
    pub(super) failure: Option<Arc<io::Error>>,
    pub(super) native: Option<Result<zero_tun::TunCloseReceipt, Arc<io::Error>>>,
}
struct State {
    task: Option<JoinHandle<DriverResult>>,
    result: Option<Result<Arc<DriverResult>, Arc<JoinError>>>,
}
pub(super) struct DriverTask {
    state: Mutex<State>,
}
impl DriverTask {
    pub(super) fn new(future: impl Future<Output = DriverResult> + Send + 'static) -> Self {
        // This is the original single driver, not an additional cleanup task.
        Self {
            state: Mutex::new(State {
                task: Some(tokio::spawn(future)),
                result: None,
            }),
        }
    }
    pub(super) fn is_finished(&self) -> bool {
        self.state.try_lock().map_or(true, |state| {
            state.result.is_some() || state.task.as_ref().is_none_or(JoinHandle::is_finished)
        })
    }
    async fn outcome(&self) -> Result<Arc<DriverResult>, io::Error> {
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
    #[cfg(test)]
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
