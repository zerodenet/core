//! Original Source workers registered directly in their one host Scope.
//! The records below retain outputs and exact witnesses, never a second JoinSet.
mod failure;
mod guard;
mod lifetime;
mod quiescence;
use super::{Source, TaskOutput};
pub(crate) use failure::SourceJoinFailure;
pub(crate) use guard::SourceTasksGuard;
use std::{
    future::Future,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use zero_core::component::TaskId;
use zero_platform_tokio::contract::{
    lifecycle::{CloseReport, ResourceLifecycle, Unfinished},
    scope::{TaskClass, TaskJoinWitness, TaskJoined, WeakTaskRegistrar},
    CancellationToken, ChannelError, CloseReason, Impact, Reason, Stage,
};
pub(crate) struct ScopedSourceTasks<T: TaskOutput> {
    inner: Arc<Inner<T>>,
}
struct Inner<T: TaskOutput> {
    label: &'static str,
    limit: usize,
    class: TaskClass,
    scope: OnceLock<WeakTaskRegistrar>,
    state: Mutex<State<T>>,
    consumer: AsyncMutex<()>,
    changed: Notify,
    stop: CancellationToken,
}
struct State<T: TaskOutput> {
    records: Vec<Arc<Record<T>>>,
    closing: bool,
    drained: bool,
    failure: Vec<Arc<SourceJoinFailure>>,
    admission: Option<Arc<ChannelError>>,
}
struct Record<T: TaskOutput> {
    witness: OnceLock<TaskJoinWitness>,
    output: Mutex<Option<T>>,
}
impl<T: TaskOutput> Clone for ScopedSourceTasks<T> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}
impl<T: TaskOutput> ScopedSourceTasks<T> {
    pub(crate) fn new(label: &'static str, limit: usize, class: TaskClass) -> Self {
        Self {
            inner: Arc::new(Inner {
                label,
                limit,
                class,
                scope: OnceLock::new(),
                state: Mutex::new(State {
                    records: Vec::new(),
                    closing: false,
                    drained: false,
                    failure: Vec::new(),
                    admission: None,
                }),
                consumer: AsyncMutex::new(()),
                changed: Notify::new(),
                stop: CancellationToken::new(),
            }),
        }
    }
    pub(crate) fn bind(&self, scope: WeakTaskRegistrar) -> Result<(), ChannelError> {
        if !self.inner.state.lock().unwrap().records.is_empty() {
            return Err(error(Reason::InvalidConfig));
        }
        self.inner
            .scope
            .set(scope)
            .map_err(|_| error(Reason::InvalidConfig))
    }
    pub(crate) async fn lock(&self) -> Result<SourceTasksGuard<'_, T>, ChannelError> {
        let lease = self.inner.consumer.lock().await;
        if self.inner.state.lock().unwrap().closing {
            return Err(error(Reason::Revoked));
        }
        Ok(SourceTasksGuard {
            owner: self,
            _lease: lease,
        })
    }
    pub(crate) fn try_lock(&self) -> Option<SourceTasksGuard<'_, T>> {
        let lease = self.inner.consumer.try_lock().ok()?;
        if self.inner.state.lock().unwrap().closing {
            return None;
        }
        Some(SourceTasksGuard {
            owner: self,
            _lease: lease,
        })
    }
    fn spawn(
        &self,
        future: impl Future<Output = T> + Send + 'static,
    ) -> Result<TaskId, ChannelError> {
        let scope = self
            .inner
            .scope
            .get()
            .ok_or_else(|| error(Reason::InvalidConfig))?;
        let host_stop = scope.cancellation().ok_or_else(|| error(Reason::Revoked))?;
        let record = Arc::new(Record {
            witness: OnceLock::new(),
            output: Mutex::new(None),
        });
        {
            let mut state = self.inner.state.lock().unwrap();
            if state.closing {
                return Err(error(Reason::Revoked));
            }
            if state.records.len() >= self.inner.limit {
                return Err(error(Reason::Capacity));
            }
            state.records.push(record.clone());
        }
        let output = record.clone();
        let owner = self.inner.clone();
        let registered = scope.spawn_tracked(self.inner.class, async move {
            let mut result = tokio::select! {biased;
                _=host_stop.cancelled()=>return Ok(()),
                _=owner.stop.cancelled()=>return Ok(()),
                output=future=>output,
            };
            // Expected Auxiliary outputs belong to their original receiver.
            // Critical driver errors also reach the actual Scope supervisor.
            let failure = if owner.class == TaskClass::Critical {
                result.preserve_failure()
            } else {
                None
            };
            *output.output.lock().unwrap() = Some(result);
            owner.changed.notify_waiters();
            failure.map_or(Ok(()), |source| {
                Err(error(Reason::Io).with_source(Reported(source)))
            })
        });
        match registered {
            Ok((id, witness)) => {
                record
                    .witness
                    .set(witness)
                    .expect("one actual Source worker registration");
                self.inner.changed.notify_waiters();
                Ok(id)
            }
            Err(source) => {
                let reason = match source {
                    zero_platform_tokio::contract::scope::SpawnError::Capacity => Reason::Capacity,
                    _ => Reason::Revoked,
                };
                let mut state = self.inner.state.lock().unwrap();
                state.records.retain(|entry| !Arc::ptr_eq(entry, &record));
                // No task was accepted or polled. Capacity denial grants no
                // replacement credit and is not a sticky cleanup fault.
                if reason != Reason::Capacity {
                    state.closing = true;
                }
                drop(state);
                Err(ChannelError::new(Stage::Prepare, reason, Impact::Component)
                    .with_source(source))
            }
        }
    }
    fn take_joined(&self) -> Option<Result<T, SourceJoinFailure>> {
        let mut state = self.inner.state.lock().unwrap();
        let position = state
            .records
            .iter()
            .position(|record| record.witness.get().is_some_and(TaskJoinWitness::is_joined))?;
        let record = state.records.swap_remove(position);
        drop(state);
        let witness = record
            .witness
            .get()
            .expect("selected actual joined registration");
        let joined = witness
            .try_joined()
            .expect("same actual consumed Scope exit");
        let output = record.output.lock().unwrap().take();
        match joined.result() {
            Err(_) => Some(Err(SourceJoinFailure::scoped(witness, joined))),
            Ok(()) => Some(output.ok_or_else(|| SourceJoinFailure::scoped(witness, joined))),
        }
    }
    async fn next_joined(&self) -> Option<Result<T, SourceJoinFailure>> {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(output) = self.take_joined() {
                return Some(output);
            }
            let records = self.inner.state.lock().unwrap().records.clone();
            if records.is_empty() {
                return None;
            }
            let mut waits = records
                .iter()
                .filter_map(|record| record.witness.get())
                .cloned()
                .map(|witness| Box::pin(async move { witness.wait().await }))
                .collect::<Vec<_>>();
            if waits.is_empty() {
                changed.await;
                continue;
            }
            tokio::select! {
                _=changed=>{},
                _=std::future::poll_fn(|cx| {
                    for wait in &mut waits {if wait.as_mut().poll(cx).is_ready(){return std::task::Poll::Ready(());}}
                    std::task::Poll::Pending
                })=>{},
            }
        }
    }
    pub(crate) fn remember(&self, failure: SourceJoinFailure) -> Arc<SourceJoinFailure> {
        let failure = Arc::new(failure);
        if !failure.is_cancelled() {
            let mut state = self.inner.state.lock().unwrap();
            state.closing = true;
            state.failure.push(failure.clone());
            drop(state);
            self.inner.stop.cancel();
            if let SourceJoinFailure::Scope {
                witness, joined, ..
            } = failure.as_ref()
            {
                if let Err(source) = witness.acknowledge_hosted_failure(joined) {
                    self.inner
                        .state
                        .lock()
                        .unwrap()
                        .admission
                        .get_or_insert_with(|| {
                            Arc::new(error(Reason::Protocol).with_source(source))
                        });
                }
            }
        }
        failure
    }
    pub(crate) fn request_abort(&self) {
        self.inner.state.lock().unwrap().closing = true;
        self.inner.stop.cancel();
    }
    #[cfg(test)]
    pub(crate) fn is_drained(&self) -> bool {
        self.inner.state.lock().unwrap().drained
    }
}
#[derive(Debug)]
struct Reported(Source);
impl std::fmt::Display for Reported {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for Reported {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}
fn error(reason: Reason) -> ChannelError {
    ChannelError::new(Stage::Close, reason, Impact::Component)
}

#[cfg(test)]
mod tests;
