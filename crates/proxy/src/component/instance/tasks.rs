//! One installed HostedScope and bounded in-flight publication capabilities.
use super::*;
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex as SyncMutex,
    },
    task::{Context, Poll},
};
use tokio::sync::Notify;
use zero_platform_tokio::contract::{
    lifecycle::{TaskClass, TaskFailure, Unfinished},
    scope::{HostedExitReceipt, HostedScope, SpawnError},
};
mod guard;
mod retirement;
pub(in crate::component) mod wake;
use wake::{MonitorWake, WaitTarget};

/// Lightweight actual fault, with no Slot, recipe, configuration or I/O owner.
#[derive(Debug)]
pub(crate) struct ComponentTaskFault {
    pub(crate) instance: ComponentInstanceId,
    pub(crate) id: TaskId,
    pub(crate) class: TaskClass,
    pub(crate) source: SharedError,
}
impl std::fmt::Display for ComponentTaskFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "component {:?} task {} ({:?}) failed: {}",
            self.instance, self.id.0, self.class, self.source
        )
    }
}
impl std::error::Error for ComponentTaskFault {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
impl ComponentTaskFault {
    pub(crate) fn error(self: &Arc<Self>) -> ChannelError {
        let mut error =
            ChannelError::new(self.source.stage, self.source.reason, self.source.impact);
        error.component = self.source.component.or(Some(self.instance.component));
        error.session = self.source.session;
        error.delivery = self.source.delivery;
        error.retry = self.source.retry;
        error.with_source(self.clone())
    }
}

pub(in crate::component) struct TaskMonitor {
    inner: SyncMutex<Tasks>,
    wake: Arc<MonitorWake>,
    pub(super) changed: Notify,
    critical: OnceLock<Arc<ComponentTaskFault>>,
    requested: AtomicU64,
    scanned: AtomicU64,
}
struct Tasks {
    scope: HostedScope,
    // Capabilities only; original completed entries still own and charge exits.
    // There can be at most the original declared task_limit such receipts.
    pending: Vec<HostedExitReceipt>,
    closing: bool,
    drained: bool,
    report: CloseReport,
}
impl TaskMonitor {
    #[cfg(test)]
    pub(super) fn registrar_for_test(&self) -> zero_platform_tokio::contract::scope::TaskRegistrar {
        self.lock().scope.registrar()
    }
    pub(super) fn new(scope: HostedScope) -> Self {
        Self {
            inner: SyncMutex::new(Tasks {
                scope,
                pending: Vec::new(),
                closing: false,
                drained: false,
                report: CloseReport::default(),
            }),
            wake: Arc::default(),
            changed: Notify::new(),
            critical: OnceLock::new(),
            requested: AtomicU64::new(0),
            scanned: AtomicU64::new(0),
        }
    }
    pub(super) fn enable_registration(&self) -> Result<(), SpawnError> {
        self.lock().scope.enable_registration()
    }
    pub(super) fn fault(&self) -> Option<Arc<ComponentTaskFault>> {
        self.critical.get().cloned()
    }
    pub(super) async fn failed(&self) -> Arc<ComponentTaskFault> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(failure) = self.fault() {
                return failure;
            }
            changed.await;
        }
    }
    pub(super) fn begin_shutdown(&self) {
        {
            let mut tasks = self.lock();
            if tasks.closing {
                return;
            }
            tasks.closing = true;
            tasks.scope.begin_shutdown();
        }
        self.changed.notify_waiters();
    }
    /// Only the catalog's elected inline driver polls this common join consumer.
    pub(super) fn poll(
        &self,
        target: &Arc<WaitTarget>,
        instance: ComponentInstanceId,
        budget: usize,
    ) -> bool {
        self.wake.bind(target);
        let waker = std::task::Waker::from(self.wake.clone());
        let mut cx = Context::from_waker(&waker);
        let epoch = self.requested.load(Ordering::Acquire);
        let mut retired = Vec::new();
        let mut progress = false;
        {
            let mut tasks = self.lock();
            for _ in 0..budget {
                let Poll::Ready(Some(receipt)) = tasks.scope.poll_exit(&mut cx) else {
                    break;
                };
                progress = true;
                match receipt.result() {
                    Ok(()) => {
                        if tasks.closing {
                            tasks.report.exited_tasks.push(receipt.id());
                        }
                        // Lifetime success IDs belong only to the real short-
                        // lived protocol/witness receiver, never a host history.
                        tasks
                            .scope
                            .acknowledge(&receipt)
                            .expect("same hosted task receipt");
                        retired.push(receipt);
                    }
                    Err(source) if receipt.class() == TaskClass::Critical => {
                        self.critical.get_or_init(|| {
                            Arc::new(ComponentTaskFault {
                                instance,
                                id: receipt.id(),
                                class: receipt.class(),
                                source,
                            })
                        });
                        tasks.pending.push(receipt);
                        tasks.closing = true;
                        tasks.scope.begin_shutdown();
                    }
                    Err(_) if receipt.local_failure_accepted() => {
                        tasks
                            .scope
                            .acknowledge(&receipt)
                            .expect("same hosted task receipt");
                        retired.push(receipt);
                    }
                    Err(_) => tasks.pending.push(receipt),
                }
            }
            // poll_exit registered the actual driver waker before this read.
            // Checking local acceptance first could lose its only wake between
            // that check and native polling (the previous wake was consumed).
            let mut index = 0;
            while index < tasks.pending.len() {
                let receipt = &tasks.pending[index];
                if receipt.class() == TaskClass::Auxiliary && receipt.local_failure_accepted() {
                    let receipt = tasks.pending.swap_remove(index);
                    tasks
                        .scope
                        .acknowledge(&receipt)
                        .expect("same hosted task receipt");
                    retired.push(receipt);
                    progress = true;
                } else {
                    index += 1;
                }
            }
            if tasks.closing && !tasks.drained && tasks.scope.is_drained() {
                tasks.drained = true;
                progress = true;
            }
        }
        // A task error's original source can own arbitrary redacted state.
        // Never drop the last publication/source reference under the host lock.
        drop(retired);
        if self.scanned.swap(epoch, Ordering::AcqRel) != epoch {
            progress = true;
        }
        if progress {
            self.changed.notify_waiters();
        }
        progress
    }
    pub(super) async fn scan(
        &self,
        deadline: Instant,
        wake: impl FnOnce(),
    ) -> Result<(), ChannelError> {
        let mut current = self.requested.load(Ordering::Acquire);
        let epoch = loop {
            let next = current
                .checked_add(1)
                .ok_or_else(|| invalid(Reason::Internal, "component scan identity exhausted"))?;
            match self.requested.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break next,
                Err(observed) => current = observed,
            }
        };
        wake();
        tokio::time::timeout_at(deadline, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(failure) = self.fault() {
                    return Err(failure.error());
                }
                if self.scanned.load(Ordering::Acquire) >= epoch {
                    return Ok(());
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| {
            invalid(
                Reason::Timeout,
                "component supervision scan remains pending",
            )
        })?
    }
}

impl ComponentSlot {
    pub(crate) fn task_fault(&self) -> Option<Arc<ComponentTaskFault>> {
        self.live
            .get()
            .and_then(|live| live.as_ref().ok())
            .and_then(|live| live.tasks.fault())
    }
    pub(in crate::component) fn wake_supervision(&self) {
        if let Some(host) = self.admission.get() {
            host.signal().notify_waiters();
        }
    }
    pub(in crate::component) fn poll_tasks(&self, target: &Arc<WaitTarget>) -> bool {
        let budget = self.recipe.spec().task_limit.map_or(1, |limit| limit.get());
        let progress = self
            .live
            .get()
            .and_then(|live| live.as_ref().ok())
            .is_some_and(|live| live.tasks.poll(target, self.instance_id(), budget));
        if self.task_fault().is_some() {
            self.revoke();
        }
        progress
    }
    pub(in crate::component) async fn scan_tasks(
        &self,
        deadline: Instant,
    ) -> Result<(), ChannelError> {
        if let Some(failure) = self.task_fault() {
            return Err(failure.error());
        }
        if self.cancel.is_cancelled() {
            return Err(self.failure(
                Reason::Revoked,
                "component was stopped before supervision scan",
            ));
        }
        if self.recipe.spec().task_limit.is_none() {
            return Ok(());
        }
        if let Some(Ok(live)) = self.live.get() {
            live.tasks.scan(deadline, || self.wake_supervision()).await
        } else {
            Err(self.failure(Reason::StalePlan, "component runtime owner was not built"))
        }
    }
}

impl Drop for TaskMonitor {
    fn drop(&mut self) {
        self.wake.unbind();
    }
}
