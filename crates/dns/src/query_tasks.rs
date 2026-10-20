//! Registration of the original DNS query jobs in an explicit owning Host Scope.
use std::{
    future::Future,
    io,
    num::NonZeroUsize,
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::{oneshot, Notify};
use zero_platform_tokio::contract::{
    scope::{TaskClass, TaskId, TaskJoinWitness, TaskRegistrar, WeakTaskRegistrar},
    CancellationToken, ChannelError,
};

tokio::task_local! {
    static QUERY_ORIGIN: TaskJoinWitness;
}

/// Query-task registration only. This weak context grants neither native I/O
/// nor service/business authority. A native provider must separately validate
/// its private root grant and the original witness's actual Scope identity.
#[derive(Clone, Debug)]
pub struct OwnedDnsQueryTasks(Arc<Shared>);
#[derive(Debug)]
struct Shared {
    registrar: WeakTaskRegistrar,
    parent: CancellationToken,
    limit: NonZeroUsize,
    state: Mutex<State>,
}
#[derive(Debug, Default)]
struct State {
    closing: bool,
    identity_fault: bool,
    records: Vec<Arc<Record>>,
    first_failure: Option<Arc<ChannelError>>,
}
#[derive(Debug)]
struct Record {
    registration: OnceLock<Option<QueryRegistration>>,
    bound: Notify,
    cancel: CancellationToken,
}
#[derive(Debug)]
struct QueryRegistration {
    id: TaskId,
    witness: TaskJoinWitness,
}
impl OwnedDnsQueryTasks {
    /// The caller supplies the actual root registrar and finite query budget.
    /// Registration still charges that original Scope's independent task limit.
    pub fn new(registrar: &TaskRegistrar, max_queries: NonZeroUsize) -> Self {
        Self(Arc::new(Shared {
            registrar: registrar.downgrade(),
            parent: registrar.cancellation(),
            limit: max_queries,
            state: Mutex::new(State::default()),
        }))
    }
    #[cfg(any(feature = "udp", test))]
    pub(crate) fn current_witness() -> io::Result<TaskJoinWitness> {
        QUERY_ORIGIN.try_with(Clone::clone).map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DNS native UDP requires its original registered query job",
            )
        })
    }
    pub(crate) fn ensure_open(&self) -> io::Result<()> {
        self.reap();
        let state = self.0.state.lock().expect("DNS query tasks poisoned");
        if let Some(source) = &state.first_failure {
            return Err(io::Error::other(source.clone()));
        }
        if state.closing || self.0.parent.is_cancelled() {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DNS query tasks are closing",
            ));
        }
        Ok(())
    }
    pub(crate) fn spawn(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
    ) -> io::Result<()> {
        self.ensure_open()?;
        let record = Arc::new(Record {
            registration: OnceLock::new(),
            bound: Notify::new(),
            cancel,
        });
        {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            if state.closing || self.0.parent.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS query admission retired",
                ));
            }
            if state.records.len() >= self.0.limit.get() {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "DNS query task capacity exhausted",
                ));
            }
            state.records.try_reserve(1).map_err(io::Error::other)?;
            // Install before the original actual spawn; an unjoined record is
            // never discarded by cancellation or a shutdown waiter timeout.
            state.records.push(record.clone());
        }
        let (sender, receiver) = oneshot::channel();
        let parent = self.0.parent.clone();
        let cancellation = record.cancel.clone();
        let callbacks = self.0.registrar.defer_callbacks();
        let spawned = self
            .0
            .registrar
            .spawn_tracked(TaskClass::Auxiliary, async move {
                let Ok(witness) = receiver.await else {
                    return Ok(());
                };
                // No original resolution/native I/O can run before the same spawn's
                // witness has been persisted and delivered by its original caller.
                QUERY_ORIGIN
                    .scope(witness, async move {
                        tokio::select! { biased;
                            _ = parent.cancelled() => {},
                            _ = cancellation.cancelled() => {},
                            _ = job => {},
                        }
                    })
                    .await;
                Ok(())
            });
        match spawned {
            Ok((id, witness)) => {
                record
                    .registration
                    .set(Some(QueryRegistration {
                        id,
                        witness: witness.clone(),
                    }))
                    .expect("query bound once");
                record.bound.notify_waiters();
                let _ = sender.send(witness);
                drop(callbacks);
                Ok(())
            }
            Err(source) => {
                // Failed synchronous registration never started a task/native.
                record
                    .registration
                    .set(None)
                    .expect("failed query registration settled once");
                record.bound.notify_waiters();
                let retired = {
                    let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
                    let index = state
                        .records
                        .iter()
                        .position(|current| Arc::ptr_eq(current, &record));
                    index.map(|index| state.records.remove(index))
                };
                record.cancel.cancel();
                drop(retired);
                drop(callbacks);
                Err(io::Error::other(source))
            }
        }
    }
    fn reap(&self) {
        loop {
            let retired = {
                let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
                let ready = state
                    .records
                    .iter()
                    .enumerate()
                    .find_map(|(index, record)| {
                        let bound = record.registration.get()?;
                        match bound {
                            None => Some((index, None)),
                            Some(registration) => registration
                                .witness
                                .try_joined()
                                .map(|joined| (index, Some((registration.id, joined)))),
                        }
                    });
                let Some((index, joined)) = ready else { return };
                if let Some((id, joined)) = joined {
                    if joined.id() != id {
                        state.closing = true;
                        state.identity_fault = true;
                        state.first_failure.get_or_insert_with(|| {
                            Arc::new(ChannelError::new(
                                zero_platform_tokio::contract::Stage::Close,
                                zero_platform_tokio::contract::Reason::Internal,
                                zero_platform_tokio::contract::Impact::Component,
                            ))
                        });
                        return; // Keep the unmatched original record charged.
                    }
                    if let Err(source) = joined.result() {
                        state.first_failure.get_or_insert(source);
                    }
                }
                state.records.remove(index)
            };
            // Actual witness/cancel/Notify destruction can invoke an external
            // waker; no final retired Record drop runs under the enclosing lock.
            drop(retired);
        }
    }
    pub(crate) async fn next_exit(&self) -> bool {
        self.reap();
        let record = {
            let state = self.0.state.lock().expect("DNS query tasks poisoned");
            if state.identity_fault {
                return false;
            }
            state.records.first().cloned()
        };
        let Some(record) = record else { return false };
        loop {
            let bound = record.bound.notified();
            tokio::pin!(bound);
            bound.as_mut().enable();
            if let Some(bound) = record.registration.get() {
                if let Some(registration) = bound {
                    registration.witness.wait().await;
                }
                self.reap();
                return true;
            }
            bound.await;
        }
    }
    /// Closes query admission and waits for actual Host Scope consumption.
    /// Does not drive that Scope, close any socket or confirm resolver users.
    pub(crate) async fn shutdown(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        let records = {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            state.closing = true;
            state.records.clone()
        };
        for record in records {
            record.cancel.cancel();
        }
        while tokio::time::timeout_at(deadline, self.next_exit())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DNS original Host Scope query join is unconfirmed",
                )
            })?
        {}
        let state = self.0.state.lock().expect("DNS query tasks poisoned");
        match &state.first_failure {
            Some(source) => Err(io::Error::other(source.clone())),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests;
