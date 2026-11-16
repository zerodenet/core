//! Explicit standalone stack fixture owns a local JoinSet. Production only
//! registers workers in the installed original Source/stack Scope.
use super::*;
impl DeviceWorkers {
    pub(in crate::runtime::raw_ip::device) fn new() -> Arc<Self> {
        Arc::new(Self {
            users: None,
            registration: Mutex::new(()),
            driver: std::sync::OnceLock::new(),
            scope: Mutex::new(ScopeBinding::default()),
            // Same configured stack ceilings also bound retiring original
            // futures until their real JoinSet consumer releases capacity.
            tasks: Some(RetainedTasks::new(
                "raw-IP client stack workers",
                WORKER_LIMIT,
            )),
        })
    }
    pub(super) fn spawn_fixture_driver(
        &self,
        driver: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<(), EngineError> {
        let Some(tasks_owner) = &self.tasks else {
            return Err(as_engine(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            )));
        };
        let _registration = self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut tasks = tasks_owner.try_lock().ok_or_else(|| {
            EngineError::Io(std::io::Error::other(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            )))
        })?;
        tasks_owner.reap_ready(&mut tasks);
        tasks
            .spawn(driver)
            .map(|_| ())
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))
    }
    pub(super) async fn join_fixture_tasks(&self) -> Result<CloseReport, EngineError> {
        let Some(tasks) = &self.tasks else {
            return Ok(CloseReport::default());
        };
        tasks
            .join_unbounded()
            .await
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))
    }
    pub(super) fn spawn_fixture_tcp(
        &self,
        worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>,
    ) -> bool {
        let Some(tasks_owner) = &self.tasks else {
            return false;
        };
        let _registration = self
            .registration
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(mut tasks) = tasks_owner.try_lock() else {
            return false;
        };
        tasks_owner.reap_ready(&mut tasks);
        tasks
            .spawn(async move {
                worker.await;
                Ok(())
            })
            .is_ok()
    }
}
