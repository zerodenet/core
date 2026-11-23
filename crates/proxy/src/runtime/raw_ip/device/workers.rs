//! Original endpoint driver and client TCP workers share one finite task owner.
#[cfg(test)]
#[path = "tests/fixture_workers.rs"]
mod fixture;
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

#[cfg(test)]
use crate::runtime::raw_ip::tasks::RetainedTasks;

// Exact per-peer ceilings: original TCP connection workers + control,
// endpoint driver, and one original packet worker per client UDP socket.
pub(super) const WORKER_LIMIT: usize = zero_stack::ClientTcpStack::BACKGROUND_TASK_LIMIT
    + 1
    + zero_stack::client_udp::ClientUdpStack::SOCKET_LIMIT;

pub(super) struct DeviceWorkers {
    users: Option<Arc<crate::runtime::packet_flow::FlowUsers>>,
    #[cfg(test)]
    tasks: Option<RetainedTasks<Result<(), EngineError>>>,
    scope: Mutex<ScopeBinding>,
    #[cfg(test)]
    registration: Mutex<()>,
    driver: std::sync::OnceLock<zero_platform_tokio::contract::scope::TaskJoinWitness>,
}
#[derive(Default)]
struct ScopeBinding {
    owner: Option<Arc<scoped::ScopedWorkers>>,
    closed: bool,
}
impl DeviceWorkers {
    pub(super) fn requires_packet_memory(&self) -> bool {
        #[cfg(test)]
        {
            self.tasks.is_none()
        }
        #[cfg(not(test))]
        {
            true
        }
    }
    pub(super) fn driver_witness(
        &self,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoinWitness> {
        self.driver.get().cloned()
    }

    /// No local JoinSet or implicit Tokio fallback exists in this branch.
    pub(super) fn suspended() -> Arc<Self> {
        Arc::new(Self {
            users: Some(crate::runtime::packet_flow::FlowUsers::new(
                std::num::NonZeroUsize::new(zero_stack::ClientTcpStack::CONNECTION_LIMIT)
                    .expect("actual TCP stack connection limit is nonzero"),
            )),
            #[cfg(test)]
            tasks: None,
            scope: Mutex::new(ScopeBinding::default()),
            #[cfg(test)]
            registration: Mutex::new(()),
            driver: std::sync::OnceLock::new(),
        })
    }
    pub(super) fn matches_scope(
        &self,
        scope: &zero_platform_tokio::contract::scope::TaskScopeIdentity,
    ) -> bool {
        self.scoped()
            .is_some_and(|owner| owner.matches_scope(scope))
    }
    pub(super) fn flow_users(&self) -> Option<Arc<crate::runtime::packet_flow::FlowUsers>> {
        self.users.clone()
    }
    pub(super) fn bind_scope(&self, registrar: WeakTaskRegistrar) -> Result<(), EngineError> {
        let mut binding = self
            .scope
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        #[cfg(test)]
        if self.tasks.is_some() {
            return Err(as_engine(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            )));
        }
        if binding.closed || binding.owner.is_some() {
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
            let witness = owner
                .spawn_tracked(async move { driver.await.map_err(driver_failure) })
                .map_err(as_engine)?;
            self.driver.set(witness).map_err(|_| {
                as_engine(ChannelError::new(
                    Stage::Prepare,
                    Reason::StalePlan,
                    Impact::Component,
                ))
            })?;
            return Ok(());
        }
        #[cfg(not(test))]
        {
            Err(as_engine(ChannelError::new(
                Stage::Prepare,
                Reason::Revoked,
                Impact::Component,
            )))
        }
        #[cfg(test)]
        {
            self.spawn_fixture_driver(driver)
        }
    }
    #[cfg(test)]
    pub(super) fn spawn_packet_worker(
        &self,
        worker: impl Future<Output = Result<(), EngineError>> + Send + 'static,
    ) -> Result<(), EngineError> {
        if let Some(owner) = self.scoped() {
            return owner
                .spawn(async move { worker.await.map_err(driver_failure) })
                .map_err(as_engine);
        }
        self.spawn_fixture_driver(worker)
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
        #[cfg(test)]
        if let Some(tasks) = &self.tasks {
            tasks.request_abort();
        }
    }
    pub(super) fn is_drained(&self) -> bool {
        #[cfg(test)]
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
        #[cfg(not(test))]
        {
            Ok(CloseReport::default())
        }
        #[cfg(test)]
        {
            self.join_fixture_tasks().await
        }
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
        #[cfg(not(test))]
        {
            false
        }
        #[cfg(test)]
        {
            self.spawn_fixture_tcp(worker)
        }
    }
}

fn as_engine(source: ChannelError) -> EngineError {
    EngineError::Io(std::io::Error::other(source))
}

fn driver_failure(source: EngineError) -> ChannelError {
    ChannelError::new(Stage::Receive, Reason::Io, Impact::Component).with_source(source)
}

#[cfg(test)]
#[path = "tests/workers.rs"]
mod tests;
