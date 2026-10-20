//! Original endpoint driver and client TCP workers share one finite task owner.
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{lifecycle::CloseReport, ChannelError, Impact, Reason, Stage};

use crate::runtime::raw_ip::tasks::RetainedTasks;

pub(super) struct DeviceWorkers {
    tasks: RetainedTasks<Result<(), EngineError>>,
    registration: Mutex<()>,
}
impl DeviceWorkers {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            registration: Mutex::new(()),
            // Existing client connection ceiling + its control worker + this
            // exact endpoint/standalone driver. No speculative extra workers.
            tasks: RetainedTasks::new(
                "raw-IP client stack workers",
                zero_stack::ClientTcpStack::BACKGROUND_TASK_LIMIT + 1,
            ),
        })
    }
    pub(super) fn spawn_driver(
        &self,
        driver: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<(), EngineError> {
        let _registration = self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut tasks = self.tasks.try_lock().ok_or_else(|| {
            EngineError::Io(std::io::Error::other(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            )))
        })?;
        self.tasks.reap_ready(&mut tasks);
        tasks
            .spawn(driver)
            .map(|_| ())
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))
    }
    pub(super) fn abort(&self) {
        self.tasks.request_abort();
    }
    pub(super) async fn join(&self) -> Result<CloseReport, EngineError> {
        self.tasks
            .join_unbounded()
            .await
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))
    }
}
impl zero_stack::TcpTaskOwner for DeviceWorkers {
    fn spawn(&self, worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        let _registration = self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(mut tasks) = self.tasks.try_lock() else {
            return false;
        };
        self.tasks.reap_ready(&mut tasks);
        tasks
            .spawn(async move {
                worker.await;
                Ok(())
            })
            .is_ok()
    }
}

#[cfg(test)]
#[path = "tests/workers.rs"]
mod tests;
