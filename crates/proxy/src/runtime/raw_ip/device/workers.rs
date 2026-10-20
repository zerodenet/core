//! Original endpoint driver and client TCP workers share one finite task owner.
mod scoped;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use zero_engine::EngineError;
use zero_platform_tokio::contract::{
    lifecycle::CloseReport, scope::WeakTaskRegistrar, ChannelError, Impact, Reason, Stage,
};

use crate::runtime::raw_ip::tasks::RetainedTasks;

pub(super) const WORKER_LIMIT: usize = zero_stack::ClientTcpStack::BACKGROUND_TASK_LIMIT + 1;

pub(super) struct DeviceWorkers {
    users: Option<Arc<crate::runtime::packet_flow::FlowUsers>>,
    tasks: Option<RetainedTasks<Result<(), EngineError>>>,
    scope: Mutex<ScopeBinding>,
    registration: Mutex<()>,
}
#[derive(Default)]
struct ScopeBinding {
    owner: Option<Arc<scoped::ScopedWorkers>>,
    closed: bool,
}
impl DeviceWorkers {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            users: None,
            registration: Mutex::new(()),
            scope: Mutex::new(ScopeBinding::default()),
            // Existing client connection ceiling + its control worker + this
            // exact endpoint/standalone driver. No speculative extra workers.
            tasks: Some(RetainedTasks::new(
                "raw-IP client stack workers",
                WORKER_LIMIT,
            )),
        })
    }
    /// No local JoinSet or implicit Tokio fallback exists in this branch.
    pub(super) fn suspended() -> Arc<Self> {
        Arc::new(Self {
            users: Some(crate::runtime::packet_flow::FlowUsers::new(
                std::num::NonZeroUsize::new(zero_stack::ClientTcpStack::CONNECTION_LIMIT)
                    .expect("actual TCP stack connection limit is nonzero"),
            )),
            tasks: None,
            scope: Mutex::new(ScopeBinding::default()),
            registration: Mutex::new(()),
        })
    }
    pub(super) fn flow_users(&self) -> Option<Arc<crate::runtime::packet_flow::FlowUsers>> {
        self.users.clone()
    }
    pub(super) fn bind_scope(&self, registrar: WeakTaskRegistrar) -> Result<(), EngineError> {
        let mut binding = self
            .scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.tasks.is_some() || binding.closed || binding.owner.is_some() {
            return Err(as_engine(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            )));
        }
        binding.owner = Some(Arc::new(scoped::ScopedWorkers::new(registrar)));
        Ok(())
    }
    fn scoped(&self) -> Option<Arc<scoped::ScopedWorkers>> {
        self.scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .owner
            .clone()
    }
    pub(super) fn spawn_driver(
        &self,
        driver: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<(), EngineError> {
        if let Some(owner) = self.scoped() {
            return owner
                .spawn(async move {
                    driver.await.map_err(|source| {
                        ChannelError::new(Stage::Receive, Reason::Io, Impact::Component)
                            .with_source(source)
                    })
                })
                .map_err(as_engine);
        }
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
    pub(super) fn first_scoped_failure(&self) -> Option<EngineError> {
        self.scoped()
            .and_then(|owner| owner.first_failure())
            .map(as_engine)
    }
    pub(super) fn abort(&self) {
        if let Some(users) = &self.users {
            users.revoke();
        }

        let owner = {
            let mut binding = self
                .scope
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            binding.closed = true;
            binding.owner.clone()
        };
        if let Some(owner) = owner {
            owner.revoke();
        }
        if let Some(tasks) = &self.tasks {
            tasks.request_abort();
        }
    }
    pub(super) fn is_drained(&self) -> bool {
        if let Some(tasks) = &self.tasks {
            return tasks.is_drained();
        }
        let binding = self
            .scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        binding
            .owner
            .as_ref()
            .map_or(binding.closed, |owner| owner.is_drained())
    }
    pub(super) async fn join(&self) -> Result<CloseReport, EngineError> {
        self.abort();
        if let Some(owner) = self.scoped() {
            return owner.join().await.map_err(as_engine);
        }
        let Some(tasks) = &self.tasks else {
            return Ok(CloseReport::default());
        };
        tasks
            .join_unbounded()
            .await
            .map_err(|source| EngineError::Io(std::io::Error::other(source)))
    }
}
impl zero_stack::TcpTaskOwner for DeviceWorkers {
    fn spawn(&self, worker: Pin<Box<dyn Future<Output = ()> + Send + 'static>>) -> bool {
        if let Some(owner) = self.scoped() {
            return owner
                .spawn(async move {
                    worker.await;
                    Ok(())
                })
                .is_ok();
        }
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

fn as_engine(source: ChannelError) -> EngineError {
    EngineError::Io(std::io::Error::other(source))
}

#[cfg(test)]
#[path = "tests/workers.rs"]
mod tests;
