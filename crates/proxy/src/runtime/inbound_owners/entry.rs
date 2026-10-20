use super::*;
use crate::component::instance::{retained, SharedError};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        OnceLock,
    },
};
use tokio::sync::Mutex as AsyncMutex;
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, TaskFailure},
    scope::{TaskClass, TaskExit, TaskRegistrar, TaskScope},
    ChannelError, CloseReason, Impact, Reason, Stage,
};
mod shutdown;
#[cfg(test)]
mod tests;
struct Resource {
    identity: ComponentInstanceId,
    owner: AsyncMutex<Box<dyn DynResourceLifecycle>>,
    stopped: AtomicBool,
}
pub(crate) struct InboundOwner {
    identity: ComponentInstanceId,
    shutdown: watch::Sender<bool>,
    cancel: CancellationToken,
    scope: AsyncMutex<TaskScope>,
    registrar: TaskRegistrar,
    resources: Mutex<Vec<Arc<Resource>>>,
    report: Mutex<CloseReport>,
    failure: Mutex<Option<SharedError>>,
    driver: OnceLock<TaskId>,
    driver_finished: AtomicBool,
}
#[derive(Clone)]
pub(crate) struct InboundTaskAccess {
    registrar: TaskRegistrar,
    cancel: CancellationToken,
    shutdown: watch::Sender<bool>,
}
impl InboundTaskAccess {
    pub(crate) fn request_stop(&self) {
        self.cancel.cancel();
        self.shutdown.send_replace(true);
    }

    pub(crate) fn cancellation(&self) -> CancellationToken {
        self.cancel.clone()
    }
    pub(crate) fn spawn(
        &self,
        work: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<TaskId, EngineError> {
        self.registrar
            .spawn(TaskClass::Critical, async move {
                work.await.map_err(|source| {
                    ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(source)
                })
            })
            .map_err(|source| std::io::Error::other(source).into())
    }
}
impl InboundOwner {
    pub(super) fn new(
        identity: ComponentInstanceId,
        limit: NonZeroUsize,
        shutdown: watch::Sender<bool>,
    ) -> Self {
        let scope = TaskScope::suspended(tokio::runtime::Handle::current(), Some(limit));
        let registrar = scope.registrar();
        Self {
            identity,
            shutdown,
            cancel: CancellationToken::new(),
            scope: AsyncMutex::new(scope),
            registrar,
            resources: Mutex::new(Vec::new()),
            report: Mutex::new(CloseReport::default()),
            failure: Mutex::new(None),
            driver: OnceLock::new(),
            driver_finished: AtomicBool::new(false),
        }
    }
    pub(super) fn install_resource(
        &self,
        identity: ComponentInstanceId,
        owner: Box<dyn DynResourceLifecycle>,
    ) {
        self.resources
            .lock()
            .expect("inbound resources poisoned")
            .push(Arc::new(Resource {
                identity,
                owner: AsyncMutex::new(owner),
                stopped: AtomicBool::new(false),
            }));
    }
    pub(crate) fn task_access(&self) -> InboundTaskAccess {
        InboundTaskAccess {
            registrar: self.registrar.clone(),
            cancel: self.cancel.clone(),
            shutdown: self.shutdown.clone(),
        }
    }
    pub(crate) fn spawn_driver(
        self: &Arc<Self>,
        work: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<(), EngineError> {
        if self.cancel.is_cancelled() || *self.shutdown.borrow() {
            return Err(std::io::Error::other("inbound stopped before driver acceptance").into());
        }
        self.scope
            .try_lock()
            .map_err(|_| std::io::Error::other("inbound owner is already joining its scope"))?
            .enable_registration()
            .map_err(std::io::Error::other)?;
        let identity = self.identity;
        let stopping = self.shutdown.clone();
        let cancel = self.cancel.clone();
        let resources = self
            .resources
            .lock()
            .expect("inbound resources poisoned")
            .clone();
        let task = self
            .registrar
            .spawn(TaskClass::Critical, async move {
                for resource in resources {
                    if cancel.is_cancelled() || *stopping.borrow() {
                        return Ok(());
                    }
                    let mut owner = resource.owner.lock().await;
                    if let Err(mut error) = owner
                        .start(Instant::now() + std::time::Duration::from_secs(5))
                        .await
                    {
                        if error.reason == Reason::Revoked
                            && (cancel.is_cancelled() || *stopping.borrow())
                        {
                            return Ok(());
                        }
                        error.component.get_or_insert(resource.identity.component);
                        return Err(error);
                    }
                }
                if cancel.is_cancelled() || *stopping.borrow() {
                    return Ok(());
                }
                work.await.map_err(|source| {
                    let mut error = ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                        .with_source(source);
                    error.component = Some(identity.component);
                    error
                })
            })
            .map_err(std::io::Error::other)?;
        self.publish_driver(task);
        Ok(())
    }
    fn publish_driver(&self, task: TaskId) {
        let report = self.report.lock().expect("inbound close report poisoned");
        self.driver.set(task).expect("one actual listener driver");
        // A concurrent shutdown may have already joined the real registered
        // task. Publish only that observed receipt, never an invented exit.
        if report.exited_tasks.contains(&task) {
            self.driver_finished.store(true, Ordering::Release);
        }
    }
    fn stop(&self) {
        self.cancel.cancel();
        self.shutdown.send_replace(true);
    }
    fn record(&self, exit: TaskExit) {
        let mut report = self.report.lock().expect("inbound close report poisoned");
        report.exited_tasks.push(exit.id);
        if let Err(mut error) = exit.result {
            error.component.get_or_insert(self.identity.component);
            let error = Arc::new(error);
            self.failure
                .lock()
                .expect("inbound failure poisoned")
                .get_or_insert(error.clone());
            report.failed_tasks.push(TaskFailure {
                id: exit.id,
                class: exit.class,
                error: retained(error),
            });
        }
        if self.driver.get() == Some(&exit.id) {
            self.driver_finished.store(true, Ordering::Release);
        }
    }
    fn result(&self) -> Result<(), EngineError> {
        match self
            .failure
            .lock()
            .expect("inbound failure poisoned")
            .as_ref()
        {
            Some(error) => Err(std::io::Error::other(retained(error.clone())).into()),
            None => Ok(()),
        }
    }
    pub(crate) async fn observe_driver(&self) -> Result<(), EngineError> {
        if self.driver.get().is_none() {
            return Err(std::io::Error::other("inbound driver was not accepted").into());
        }
        let mut scope = self.scope.lock().await;
        while !self.driver_finished.load(Ordering::Acquire) {
            let exit = scope
                .next_exit()
                .await
                .ok_or_else(|| std::io::Error::other("listener driver exit remains unobserved"))?;
            let failed = exit.result.is_err();
            self.record(exit); // persist the actual ID/source before any next await
            if failed {
                self.stop();
            }
        }
        self.result()
    }
}
