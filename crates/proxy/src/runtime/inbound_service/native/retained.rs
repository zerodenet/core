//! Actual Source lifecycles and accepted graphs outlive every borrowed waiter.
use super::*;
use std::sync::Mutex;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::ResourceLifecycle, scope::TaskJoinWitness, CloseReason,
};
struct Lifecycle {
    inner: Box<dyn DynResourceLifecycle>,
    started: bool,
    receipts: zero_platform_tokio::contract::lifecycle::CloseReport,
}
struct Group {
    graph: zero_platform_tokio::AcceptedPeerScope,
    primary: TaskJoinWitness,
}
#[derive(Default)]
pub(in crate::runtime::inbound_service) struct Retained {
    lifecycles: Mutex<Vec<Arc<tokio::sync::Mutex<Lifecycle>>>>,
    groups: Mutex<Vec<Arc<Group>>>,
    pub(in crate::runtime::inbound_service) peers:
        Arc<Mutex<Vec<Arc<crate::runtime::inbound_owners::InboundPeerTasks>>>>,
    failures: Mutex<Vec<zero_platform_tokio::AcceptedTaskFailures>>,
}
impl Retained {
    pub(in crate::runtime::inbound_service) fn install(
        &self,
        lifecycle: Box<dyn DynResourceLifecycle>,
        failures: zero_platform_tokio::AcceptedTaskFailures,
    ) {
        self.failures.lock().unwrap().push(failures);
        self.lifecycles
            .lock()
            .unwrap()
            .push(Arc::new(tokio::sync::Mutex::new(Lifecycle {
                inner: lifecycle,
                started: false,
                receipts: Default::default(),
            })));
    }
    pub(in crate::runtime::inbound_service) fn install_group(
        &self,
        graph: zero_platform_tokio::AcceptedPeerScope,
        primary: TaskJoinWitness,
    ) {
        self.groups
            .lock()
            .unwrap()
            .push(Arc::new(Group { graph, primary }));
    }
    pub(in crate::runtime::inbound_service) fn observe_joined(
        &self,
        task: zero_core::component::TaskId,
    ) -> Option<zero_platform_tokio::contract::scope::TaskJoined> {
        let groups = self.groups.lock().unwrap().clone();
        let accepted = groups
            .iter()
            .find_map(|group| group.graph.observe_joined(task));
        let peers = self.peers.lock().unwrap().clone();
        let peer = peers
            .iter()
            .find_map(|group| group.receive_service_joined(task));
        peer.or(accepted)
    }
    pub(in crate::runtime::inbound_service) async fn start(
        &self,
        deadline: Instant,
    ) -> Result<(), EngineError> {
        let originals = self.lifecycles.lock().unwrap().clone();
        for original in originals {
            let mut lifecycle = original.lock().await;
            if !lifecycle.started {
                lifecycle
                    .inner
                    .start(deadline)
                    .await
                    .map_err(std::io::Error::other)?;
                lifecycle.started = true;
            }
        }
        Ok(())
    }
    pub(in crate::runtime::inbound_service) async fn reap(&self) -> Result<(), EngineError> {
        let peers = self.peers.lock().unwrap().clone();
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        for group in peers {
            group
                .retire_logical_resources(deadline)
                .await
                .map_err(std::io::Error::other)?;
            group
                .retire_joined(deadline)
                .await
                .map_err(std::io::Error::other)?;
        }
        self.retire_groups(deadline, false).await
    }
    async fn retire_groups(&self, deadline: Instant, all: bool) -> Result<(), EngineError> {
        let groups = self.groups.lock().unwrap().clone();
        for original in groups {
            if !all && !original.primary.is_joined() {
                continue;
            }
            let mut graph = original.graph.clone();
            let report = graph
                .shutdown(deadline)
                .await
                .map_err(std::io::Error::other)?;
            if report.is_complete() {
                self.groups
                    .lock()
                    .unwrap()
                    .retain(|entry| !Arc::ptr_eq(entry, &original));
            }
        }
        Ok(())
    }
    pub(in crate::runtime::inbound_service) async fn revoke(&self) {
        let peers = self.peers.lock().unwrap().clone();
        for group in peers {
            group.begin_shutdown();
        }
        let originals = self.lifecycles.lock().unwrap().clone();
        for original in originals {
            original.lock().await.inner.revoke(CloseReason::Shutdown);
        }
    }
    pub(in crate::runtime::inbound_service) async fn close(
        &self,
        deadline: Instant,
    ) -> Result<(), EngineError> {
        let peers = self.peers.lock().unwrap().clone();
        for group in peers {
            tokio::time::timeout_at(deadline, group.wait_closed())
                .await
                .map_err(|_| {
                    std::io::Error::new(
                        std::io::ErrorKind::TimedOut,
                        "active UDP association original cleanup remains pending",
                    )
                })??;
            if !group.physically_closed() {
                return Err(
                    std::io::Error::other("active association custody remains pending").into(),
                );
            }
            self.peers
                .lock()
                .unwrap()
                .retain(|entry| !Arc::ptr_eq(entry, &group));
        }
        self.retire_groups(deadline, true).await?;
        if !self.groups.lock().unwrap().is_empty() {
            return Err(std::io::Error::other(
                "active accepted graphs still require original joins/resources",
            )
            .into());
        }
        let originals = self.lifecycles.lock().unwrap().clone();
        for original in originals {
            let mut lifecycle = original.lock().await;
            let report = lifecycle
                .inner
                .shutdown(deadline)
                .await
                .map_err(std::io::Error::other)?;
            let complete = report.is_complete();
            lifecycle.receipts.unfinished.clear();
            lifecycle.receipts.append(report);
            let failed = !lifecycle.receipts.failed_tasks.is_empty()
                || !lifecycle.receipts.resource_failures.is_empty();
            if failed {
                return Err(std::io::Error::other(
                    "active Source retained original logical cleanup failures",
                )
                .into());
            }
            if !complete {
                return Err(std::io::Error::other(
                    "active Source logical loan cleanup remains unconfirmed",
                )
                .into());
            }
            drop(lifecycle);
            self.lifecycles
                .lock()
                .unwrap()
                .retain(|entry| !Arc::ptr_eq(entry, &original));
        }
        let failures = std::mem::take(&mut *self.failures.lock().unwrap());
        for receiver in failures {
            for failure in receiver.iter() {
                tracing::debug!(task_id=failure.id().0,task_class=?failure.class(),error=?failure.result().err(),"active Source retained original client failure after logical closure");
            }
        }
        Ok(())
    }
    pub(in crate::runtime::inbound_service) fn is_empty(&self) -> bool {
        self.peers.lock().unwrap().is_empty()
            && self.groups.lock().unwrap().is_empty()
            && self.lifecycles.lock().unwrap().is_empty()
    }
}
