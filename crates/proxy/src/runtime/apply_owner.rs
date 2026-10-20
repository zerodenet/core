//! One persistent owner for the already serialized configuration transaction.
//! Waiting callers are outside acceptance; an accepted task outlives its waiter.
use std::{
    future::Future,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{Mutex, Notify},
    time::{timeout_at, Instant},
};
use zero_platform_tokio::contract::{
    lifecycle::{TaskClass, TaskFailure},
    scope::{TaskExit, TaskScope},
};

#[derive(Default)]
pub(super) struct ApplyOwner {
    closing: AtomicBool,
    completed: AtomicBool,
    changed: Notify,
    commit_gate: std::sync::Mutex<()>,
    owner: Mutex<Owner>,
}
#[derive(Default)]
struct Owner {
    scope: Option<TaskScope>,
    occupied: bool,
    failure: Option<TaskFailure>,
}
struct Completion(Arc<ApplyOwner>);
impl Drop for Completion {
    fn drop(&mut self) {
        self.0.completed.store(true, Ordering::Release);
        self.0.changed.notify_one();
    }
}
impl Owner {
    fn record(&mut self, exit: TaskExit) {
        self.occupied = false;
        if let Err(error) = exit.result {
            self.failure.get_or_insert(TaskFailure {
                id: exit.id,
                class: exit.class,
                error,
            });
        }
    }
    fn result(&self) -> Result<(), String> {
        self.failure.as_ref().map_or(Ok(()), |failure| {
            Err(format!(
                "configuration transaction task {} failed: {}",
                failure.id.0, failure.error
            ))
        })
    }
    async fn join(&mut self) -> Result<(), String> {
        if self.occupied {
            let exit = self
                .scope
                .as_mut()
                .expect("accepted transaction scope")
                .next_exit()
                .await
                .ok_or_else(|| "configuration transaction exit was not observed".to_owned())?;
            self.record(exit);
        }
        self.result()
    }
}
impl ApplyOwner {
    pub(super) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }
    pub(super) fn begin_shutdown(&self) {
        let _gate = self
            .commit_gate
            .lock()
            .expect("configuration commit gate poisoned");
        self.closing.store(true, Ordering::Release);
        self.changed.notify_one();
    }
    pub(super) fn commit(&self, commit: impl FnOnce() -> Result<(), String>) -> Result<(), String> {
        let _gate = self
            .commit_gate
            .lock()
            .expect("configuration commit gate poisoned");
        if self.is_closing() {
            return Err("proxy stopped before configuration commit".to_owned());
        }
        commit()
    }
    pub(super) async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        serial: Arc<Mutex<()>>,
        work: impl Future<Output = Result<T, String>> + Send + 'static,
    ) -> Result<T, String> {
        if self.is_closing() {
            return Err("proxy configuration owner is stopping".to_owned());
        }
        // No mutation has been accepted while this caller waits. Once acquired,
        // this exact guard moves into the owned task before the first await.
        let guard = serial.lock_owned().await;
        let mut owner = self.owner.lock().await;
        owner.join().await?;
        if self.is_closing() {
            return Err("proxy configuration owner is stopping".to_owned());
        }
        let scope = owner.scope.get_or_insert_with(|| {
            TaskScope::new(
                tokio::runtime::Handle::current(),
                NonZeroUsize::new(1).unwrap(),
            )
        });
        let (send, receive) = tokio::sync::oneshot::channel();
        let completion = Completion(self.clone());
        scope
            .registrar()
            .spawn(TaskClass::Critical, async move {
                let _completion = completion;
                let _guard = guard;
                let result = work.await;
                // A lost acknowledgement does not cancel commit or compensation.
                let _ = send.send(result);
                Ok(())
            })
            .map_err(|source| format!("configuration transaction was not accepted: {source}"))?;
        owner.occupied = true;
        drop(owner);
        receive.await.map_err(|_| {
            "configuration transaction ended without an acknowledgement; query current state"
                .to_owned()
        })?
    }
    pub(super) async fn wait_idle(&self) -> Result<(), String> {
        self.owner.lock().await.join().await
    }
    /// The run loop polls this alongside reload reconciliation. Dropping this
    /// polling future retains the real scope and any collected task failure.
    pub(super) async fn observe_failure(&self) -> String {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.completed.swap(false, Ordering::AcqRel) {
                if let Err(error) = self.wait_idle().await {
                    return error;
                }
            }
            changed.await;
        }
    }
    pub(super) async fn shutdown(&self, deadline: Instant) -> Result<(), String> {
        self.begin_shutdown();
        timeout_at(deadline, self.wait_idle())
            .await
            .map_err(|_| "configuration transaction shutdown remains unconfirmed".to_owned())??;
        let mut owner = self.owner.lock().await;
        if let Some(scope) = owner.scope.as_mut() {
            let report = scope.shutdown(deadline).await;
            if !report.is_complete() || !report.failed_tasks.is_empty() {
                return Err("configuration transaction task cleanup remains incomplete".to_owned());
            }
        }
        owner.result()
    }
}

#[cfg(test)]
#[path = "tests/apply_owner.rs"]
mod tests;
