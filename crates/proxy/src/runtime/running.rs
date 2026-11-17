use std::io;
use std::ops::Deref;

use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use zero_engine::{Engine, EngineError};

use super::Proxy;

pub struct RunningProxy {
    pub(super) proxy: Proxy,
    pub(super) shutdown: Option<oneshot::Sender<()>>,
    pub(super) task: JoinHandle<Result<(), EngineError>>,
}

impl RunningProxy {
    pub fn engine(&self) -> &Engine {
        self.proxy.engine()
    }

    /// Observe the original startup publication, after Sources are ready and
    /// the actual component generation is committed. A bound port alone can
    /// precede that publication. This grants no native or task close receipt.
    pub async fn wait_ready(&self, deadline: tokio::time::Instant) -> Result<(), EngineError> {
        let mut ready = self.proxy.orchestration_ready.subscribe();
        tokio::time::timeout_at(deadline, async {
            loop {
                if *ready.borrow_and_update() {
                    return Ok(());
                }
                if self.task.is_finished() {
                    return Err(io::Error::other("proxy stopped before startup publication").into());
                }
                ready
                    .changed()
                    .await
                    .map_err(|source| EngineError::Io(io::Error::other(source)))?;
            }
        })
        .await
        .map_err(|source| EngineError::Io(io::Error::new(io::ErrorKind::TimedOut, source)))?
    }

    pub async fn shutdown(mut self) -> Result<(), EngineError> {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }

        self.task
            .await
            .map_err(|error| EngineError::from(io::Error::other(error)))?
    }
}

impl Deref for RunningProxy {
    type Target = Engine;

    fn deref(&self) -> &Self::Target {
        self.proxy.engine()
    }
}
