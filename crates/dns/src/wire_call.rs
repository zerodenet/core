//! One real wire caller observes its existing coalesced Query owner.
//! This contains no resolver, scheduler, native grant or additional worker.
use crate::DnsSystem;
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
}
struct Returned(Arc<State>);
impl Drop for Returned {
    fn drop(&mut self) {
        // Return of this one caller future, never a task/native close receipt.
        self.0.returned.store(true, Ordering::Release);
        self.0.changed.notify_waiters();
    }
}
impl DnsSystem {
    /// Persist the control before polling the returned single-use answer future.
    /// Its Drop returns only this waiter; a shared query keeps its other callers.
    pub fn prepare_udp_query<'a>(
        &'a self,
        query: &'a [u8],
    ) -> (DnsQueryCall, BoxFuture<'a, io::Result<Vec<u8>>>) {
        self.prepare_wire_query(query, false)
    }
    /// The same actual caller/worker ownership for DNS-over-TCP framing.
    pub fn prepare_tcp_query<'a>(
        &'a self,
        query: &'a [u8],
    ) -> (DnsQueryCall, BoxFuture<'a, io::Result<Vec<u8>>>) {
        self.prepare_wire_query(query, true)
    }
    fn prepare_wire_query<'a>(
        &'a self,
        query: &'a [u8],
        tcp: bool,
    ) -> (DnsQueryCall, BoxFuture<'a, io::Result<Vec<u8>>>) {
        let state = Arc::new(State {
            cancel: CancellationToken::new(),
            returned: AtomicBool::new(false),
            registration: Mutex::new(None),
            conflict: AtomicBool::new(false),
            changed: Notify::new(),
            closed: OnceLock::new(),
            cleanup_failure: OnceLock::new(),
        });
        let returned = Returned(state.clone());
        let original = DnsQueryCall(state.clone());
        let work = Box::pin(async move {
            let _returned = returned;
            let cancel = state.cancel.clone();
            CALL.scope(state, async move {
                tokio::select! { biased;
                    _ = cancel.cancelled() => Err(io::Error::new(io::ErrorKind::Interrupted,"DNS wire caller cancelled")),
                    result = async { if tcp {self.answer_tcp_query(query).await} else {self.answer_udp_query(query).await} } => result,
                }
            }).await
        });
        (original, work)
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
            let (query, joined, failure) = match registration {
                None => (None, None, None),
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
                        Admission::NoWorker => (None, None, None),
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
                            if let Err(source) = provider.finish_query(query.clone()).await {
                                let _ = self.0.cleanup_failure.set(Arc::new(source));
                            }
                            if !tasks.native_retirement_complete(retirement, query) {
                                return Err(match self.0.cleanup_failure.get() {
                                    Some(source)=>io::Error::new(source.kind(),source.clone()),
                                    None=>io::Error::other("DNS original query/descendant native retirement is unconfirmed"),
                                });
                            }
                            let failure = joined.result().err();
                            (Some(query.clone()), Some(joined), failure)
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
