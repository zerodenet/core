//! One real wire caller observes its existing coalesced Query owner.
//! This contains no resolver, scheduler, native grant or additional worker.
use crate::DnsSystem;
mod prepared;
mod registration;
use registration::Admission;
pub(crate) use registration::Registration;
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Mutex, OnceLock},
};
use tokio::sync::Notify;
use zero_platform_tokio::contract::{
    scope::{TaskJoinWitness, TaskJoined},
    BoxFuture, CancellationToken, ChannelError,
};

tokio::task_local! { static CALL: Arc<State>; }
#[derive(Clone)]
pub struct DnsQueryCall(Arc<State>);
struct State {
    cancel: CancellationToken,
    returned: AtomicBool,
    registration: Mutex<Option<Arc<Registration>>>,
    conflict: AtomicBool,
    changed: Notify,
    closed: OnceLock<Arc<DnsQueryCallClosed>>,
    cleanup_failure: OnceLock<Arc<io::Error>>,
}
impl std::fmt::Debug for DnsQueryCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsQueryCall")
            .field("caller_returned", &self.0.returned.load(Ordering::Acquire))
            .field("retired", &self.0.closed.get().is_some())
            .finish_non_exhaustive()
    }
}
impl std::fmt::Debug for DnsQueryCallClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsQueryCallClosed")
            .field("query", &self.query)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}
/// Query-caller retirement only. A persistent DNS transport's physical original
/// remains with its actual provider and is not closed by this receipt.
pub struct DnsQueryCallClosed {
    call: std::sync::Weak<State>,
    query: Option<TaskJoinWitness>,
    joined: Option<TaskJoined>,
    failure: Option<Arc<ChannelError>>,
    cleanup_failure: Option<Arc<io::Error>>,
    cleanup_failure_registration: Option<TaskJoinWitness>,
    failure_receiver: Option<crate::OwnedDnsQueryTasks>,
}
impl DnsQueryCallClosed {
    pub fn matches(&self, call: &DnsQueryCall) -> bool {
        self.call.ptr_eq(&Arc::downgrade(&call.0))
    }
    /// None means execution acquired no Query worker or native DNS transport.
    pub fn query(&self) -> Option<&TaskJoinWitness> {
        self.query.as_ref()
    }
    pub fn joined(&self) -> Option<&TaskJoined> {
        self.joined.as_ref()
    }
    pub fn failure(&self) -> Option<&Arc<ChannelError>> {
        self.failure.as_ref()
    }
    /// Retained typed cleanup fault, independent of positive physical retirement.
    pub fn cleanup_failure(&self) -> Option<&Arc<io::Error>> {
        self.cleanup_failure.as_ref()
    }
    /// Present only when this exact cleanup source was returned by the original
    /// Query task, before its real Scope failure publication. A late cleanup
    /// fault needs a separate explicit receiver; this never invents a task ID.
    pub fn cleanup_failure_registration(&self) -> Option<&TaskJoinWitness> {
        self.cleanup_failure_registration.as_ref()
    }
    /// Canonical local delivery only. The original Host task credit and
    /// native/provider closure are separate and are never inferred here.
    pub fn cleanup_failure_accepted(&self) -> bool {
        self.cleanup_failure.is_none()
            || self
                .cleanup_failure_registration
                .as_ref()
                .is_some_and(|query| {
                    self.failure_receiver.as_ref().is_some_and(|receiver| {
                        receiver.retains_query_failure(query, self.cleanup_failure.as_ref())
                    }) && query
                        .try_joined()
                        .is_some_and(|joined| joined.failure_accepted())
                })
    }
    pub fn failure_accepted(&self) -> bool {
        self.failure.is_none()
            || self.query.as_ref().is_some_and(|query| {
                self.failure_receiver
                    .as_ref()
                    .is_some_and(|receiver| receiver.retains_query_failure(query, None))
                    && query
                        .try_joined()
                        .is_some_and(|joined| joined.failure_accepted())
            })
    }
}
struct Returned(Arc<State>);
impl Drop for Returned {
    fn drop(&mut self) {
        // Return of this one caller future, never a task/native close receipt.
        self.0.returned.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
/// Only the original wire answer can attach a flight, before its real admission.
/// A single DNS wire question selects one coordinated worker; nested bootstrap
/// jobs remain descendants of that same registered owner, not a second call.
pub(crate) fn register(registration: &Arc<Registration>) -> io::Result<()> {
    CALL.try_with(|call| {
        let mut selected = call.registration.lock().expect("DNS caller poisoned");
        match &*selected {
            Some(old) if !Arc::ptr_eq(old, registration) => {
                call.conflict.store(true, Ordering::Release);
                Err(io::Error::other(
                    "DNS wire caller selected multiple original flights",
                ))
            }
            Some(_) => Ok(()),
            None => {
                *selected = Some(registration.clone());
                Ok(())
            }
        }
    })
    .unwrap_or(Ok(()))
}
impl DnsQueryCall {
    pub fn cancel(&self) {
        self.0.cancel.cancel();
    }
    pub fn same_call(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn closed(&self) -> Option<Arc<DnsQueryCallClosed>> {
        self.0.closed.get().cloned()
    }
    pub async fn wait_closed(
        &self,
        end: tokio::time::Instant,
    ) -> io::Result<Arc<DnsQueryCallClosed>> {
        if let Some(closed) = self.closed() {
            return Ok(closed);
        }
        let work = async {
            loop {
                let changed = self.0.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.0.returned.load(Ordering::Acquire) {
                    break;
                }
                changed.await;
            }
            if self.0.conflict.load(Ordering::Acquire) {
                return Err(io::Error::other(
                    "DNS caller has unknown original flight coverage",
                ));
            }
            let registration = self
                .0
                .registration
                .lock()
                .expect("DNS caller poisoned")
                .clone();
            let (query, joined, failure, cleanup_failure_registration, failure_receiver) =
                match registration {
                    None => (None, None, None, None, None),
                    Some(registration) => {
                        let admission = loop {
                            let changed = registration.changed.notified();
                            tokio::pin!(changed);
                            changed.as_mut().enable();
                            if let Some(admission) = registration.admission.get() {
                                break admission;
                            }
                            changed.await;
                        };
                        match admission {
                            Admission::NoWorker => (None, None, None, None, None),
                            Admission::Unowned => {
                                return Err(io::Error::other(
                                    "DNS wire worker has no matching installed Host provider",
                                ))
                            }
                            Admission::Hosted {
                                query,
                                provider,
                                tasks,
                                retirement,
                            } => {
                                query.wait().await;
                                let joined = query.try_joined().ok_or_else(|| {
                                    io::Error::other("DNS original query join remains unknown")
                                })?;
                                // Retry this original provider's actual ledger even after a
                                // worker panic/finalizer error. No observer substitutes for
                                // same-Scope logical/native/descendant cleanup authority.
                                if joined.result().is_err() {
                                    tasks.retain_query_failure(retirement, query)?;
                                }
                                // A healthy completed query does not sweep unrelated
                                // provider entries or turn their later errors into its own.
                                if !tasks.native_retirement_complete(retirement, query) {
                                    if let Err(source) = provider.finish_query(query.clone()).await
                                    {
                                        let source = tasks
                                            .retain_cleanup_failure(retirement, query, source)?;
                                        let _ = self.0.cleanup_failure.set(source);
                                    }
                                }
                                let canonical_cleanup = retirement.cleanup_failure();
                                if let Some(source) = canonical_cleanup.as_ref() {
                                    let _ = self.0.cleanup_failure.set(source.clone());
                                }
                                if !tasks.native_retirement_complete(retirement, query) {
                                    return Err(match self.0.cleanup_failure.get() {
                                    Some(source)=>io::Error::new(source.kind(),source.clone()),
                                    None=>io::Error::other("DNS original query/descendant native retirement is unconfirmed"),
                                });
                                }
                                let failure = joined.result().err();
                                let cleanup_failure_registration = canonical_cleanup
                                    .as_ref()
                                    .filter(|source| {
                                        self.0
                                            .cleanup_failure
                                            .get()
                                            .is_some_and(|stored| Arc::ptr_eq(source, stored))
                                    })
                                    .map(|_| query.clone());
                                (
                                    Some(query.clone()),
                                    Some(joined),
                                    failure,
                                    cleanup_failure_registration,
                                    Some(tasks.clone()),
                                )
                            }
                        }
                    }
                };
            Ok(self
                .0
                .closed
                .get_or_init(|| {
                    Arc::new(DnsQueryCallClosed {
                        call: Arc::downgrade(&self.0),
                        query,
                        joined,
                        failure,
                        cleanup_failure: self.0.cleanup_failure.get().cloned(),
                        cleanup_failure_registration,
                        failure_receiver,
                    })
                })
                .clone())
        };
        tokio::time::timeout_at(end, work).await.map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "DNS original caller/query cleanup is unconfirmed",
            )
        })?
    }
}

#[cfg(test)]
#[path = "wire_call/tests.rs"]
mod tests;
