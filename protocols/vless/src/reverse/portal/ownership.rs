//! One original driver and real logical-view debt, without another executor.
use super::*;
use std::sync::atomic::AtomicUsize;
use tokio::time::Instant;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    scope::{TaskJoinWitness, TaskJoined},
    ChannelError, CloseReason,
};
#[derive(Default)]
pub(super) struct Custody {
    users: AtomicUsize,
    published: AtomicBool,
    source_uses: std::sync::OnceLock<zero_platform_tokio::controlled_carrier::AcceptedCarrierUses>,
    primary: std::sync::OnceLock<TaskJoinWitness>,
    pub(super) driver: Mutex<Option<TaskJoinWitness>>,
    library: tokio::sync::Mutex<Option<tokio::task::JoinHandle<()>>>,
    library_joined: AtomicBool,
    received: Mutex<Option<TaskJoined>>,
    complete: AtomicBool,
    changed: tokio::sync::Notify,
}
impl Custody {
    pub(super) fn source_uses(
        &self,
        uses: zero_platform_tokio::controlled_carrier::AcceptedCarrierUses,
    ) {
        let _ = self.source_uses.set(uses);
    }
    pub(super) fn publish(&self) {
        self.published.store(true, Ordering::Release);
    }
    pub(super) fn primary(&self, witness: TaskJoinWitness) {
        let _ = self.primary.set(witness);
    }
    pub(super) fn hosted(&self, witness: TaskJoinWitness) {
        *self.driver.lock().unwrap() = Some(witness);
    }
    pub(super) fn library(&self, task: tokio::task::JoinHandle<()>) {
        *self.library.try_lock().expect("installed synchronously") = Some(task);
    }
    pub(super) fn lease(
        self: &Arc<Self>,
        actual: Option<zero_platform_tokio::controlled_carrier::AcceptedCarrierUse>,
    ) -> Lease {
        self.users.fetch_add(1, Ordering::AcqRel);
        Lease {
            custody: self.clone(),
            _source: actual,
        }
    }
    pub(super) fn confirmed(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }
    async fn close(&self, end: Instant) -> Result<CloseReport, ChannelError> {
        // This owner covers logical views only. Before the publication gate
        // no such view exists; the actual registered task, its IO and Source
        // native controller still require their independent original receipts.
        let returned = self.source_uses.get().is_none_or(|uses| uses.returned());
        if !self.published.load(Ordering::Acquire)
            && self.users.load(Ordering::Acquire) == 0
            && returned
        {
            self.complete.store(true, Ordering::Release);
            return Ok(CloseReport::default());
        }
        let joined = self
            .driver
            .lock()
            .unwrap()
            .as_ref()
            .and_then(TaskJoinWitness::try_joined);
        let driver_joined = if let Some(joined) = joined {
            *self.received.lock().unwrap() = Some(joined.clone());
            if let Err(source) = joined.result() {
                return Err(super::component::error(
                    zero_platform_tokio::contract::Stage::Close,
                    zero_platform_tokio::contract::Reason::Io,
                )
                .with_source(source));
            }
            true
        } else {
            self.library_joined.load(Ordering::Acquire)
        };
        if !driver_joined || self.users.load(Ordering::Acquire) != 0 || !returned {
            return Ok(CloseReport {
                unfinished: vec![Unfinished::Resource(
                    "Rvs original driver join or logical views".into(),
                )],
                ..Default::default()
            });
        }
        let _ = end;
        self.complete.store(true, Ordering::Release);
        Ok(CloseReport::default())
    }
    pub(super) async fn join_library(&self, end: Instant) -> Result<(), ChannelError> {
        let mut task = self.library.lock().await;
        if let Some(original) = task.as_mut() {
            tokio::time::timeout_at(end, original)
                .await
                .map_err(|source| {
                    super::component::error(
                        zero_platform_tokio::contract::Stage::Close,
                        zero_platform_tokio::contract::Reason::Timeout,
                    )
                    .with_source(source)
                })?
                .map_err(|source| {
                    super::component::error(
                        zero_platform_tokio::contract::Stage::Close,
                        zero_platform_tokio::contract::Reason::Io,
                    )
                    .with_source(source)
                })?;
            task.take();
            self.library_joined.store(true, Ordering::Release);
        }
        Ok(())
    }
}
pub(crate) struct Lease {
    custody: Arc<Custody>,
    _source: Option<zero_platform_tokio::controlled_carrier::AcceptedCarrierUse>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.custody.users.fetch_sub(1, Ordering::AcqRel);
        self.custody.changed.notify_waiters();
    }
}
pub(super) struct WorkerOwner(pub Arc<Custody>, pub Arc<MuxPoolConn>);
impl ResourceLifecycle for WorkerOwner {
    async fn start(&mut self, _: Instant) -> Result<(), ChannelError> {
        Ok(())
    }
    fn revoke(&self, _: CloseReason) {
        if let Some(uses) = self.0.source_uses.get() {
            uses.request_stop();
        }
        self.1.close();
    }
    async fn shutdown(&mut self, end: Instant) -> Result<CloseReport, ChannelError> {
        self.0.close(end).await
    }
}
impl Worker {
    pub(super) async fn close(&self, end: Instant) -> Result<bool, ChannelError> {
        self.connection.close();
        if let Some(resource) = self.resource.get() {
            return resource.close(end).await;
        }
        self.custody.join_library(end).await?;
        Ok(self.custody.close(end).await?.is_complete())
    }
}
