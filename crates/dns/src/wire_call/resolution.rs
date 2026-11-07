//! Two actual address-family originals for one existing resolution algorithm.
//! Observation only: no resolver, native authority, executor, pool or task.
use super::*;
use crate::{
    coordinator::{QueryCoordinator, QueryScope},
    DnsSystem,
};
use std::future::Future;
mod closed;
pub use closed::{DnsResolutionClosed, DnsResolutionUnsettled};
tokio::task_local! {static RESOLUTION:Arc<Resolution>;}
#[derive(Clone)]
pub struct DnsResolutionCall(Arc<Resolution>);
struct Resolution {
    // Retain the same actual coordinator Arc, preventing identity/address ABA.
    coordinator: QueryCoordinator<Vec<zero_traits::IpAddress>>,
    node: Option<QueryCoordinator<Vec<zero_traits::IpAddress>>>,
    selection: Mutex<Option<(String, QueryScope)>>,
    calls: [DnsQueryCall; 2],
    stop: CancellationToken,
    started: AtomicBool,
    conflict: AtomicBool,
    closed: OnceLock<Arc<DnsResolutionClosed>>,
}
impl std::fmt::Debug for DnsResolutionCall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsResolutionCall")
            .field("calls", &self.0.calls)
            .finish_non_exhaustive()
    }
}
impl DnsSystem {
    /// Observe this system's same Default/Direct/Node resolution work. The caller
    /// must keep this control before polling its existing single-use algorithm.
    pub fn resolution_call(&self) -> DnsResolutionCall {
        DnsResolutionCall(Arc::new(Resolution {
            coordinator: self.query_coordinator.clone(),
            node: self
                .node_queries
                .as_ref()
                .map(|node| node.coordinator.clone()),
            selection: Mutex::new(None),
            calls: [DnsQueryCall(State::new()), DnsQueryCall(State::new())],
            stop: CancellationToken::new(),
            started: AtomicBool::new(false),
            conflict: AtomicBool::new(false),
            closed: OnceLock::new(),
        }))
    }
}
impl DnsResolutionCall {
    pub fn same_call(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
    pub fn cancel(&self) {
        self.0.stop.cancel();
    }
    /// Pure context for the original resolution/route-trace future; this spawns
    /// nothing and grants no permission to resolve or open a resource. A second
    /// scope is rejected before polling its body. Stop detaches only this caller.
    pub fn scope<T>(&self, work: impl Future<Output = T>) -> impl Future<Output = io::Result<T>> {
        // This exact prepared future owns the original caller-return guards even
        // when admission fails before its first poll. A second attempt cannot
        // return the first attempt's guards or claim its original coverage.
        let original = self.clone();
        let returned = (!self.0.started.swap(true, Ordering::AcqRel)).then(|| {
            [
                Returned(self.0.calls[0].0.clone()),
                Returned(self.0.calls[1].0.clone()),
            ]
        });
        async move {
            let Some(_returned) = returned else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "DNS resolution caller was already used",
                ));
            };
            RESOLUTION.scope(original.0.clone(),async{
                tokio::select!{biased;_=original.0.stop.cancelled()=>Err(io::Error::new(io::ErrorKind::Interrupted,"DNS resolution caller cancelled")),result=work=>Ok(result)}
            }).await
        }
    }
    pub fn closed(&self) -> Option<Arc<DnsResolutionClosed>> {
        self.0.closed.get().cloned()
    }
}
pub(crate) fn register(
    registration: &Arc<Registration>,
    coordinator: usize,
    domain: &str,
    query_type: u16,
    scope: QueryScope,
    plain: bool,
) -> io::Result<()> {
    RESOLUTION
        .try_with(|resolution| {
            let index = match query_type {
                0 | crate::message::TYPE_A => 0,
                crate::message::TYPE_AAAA => 1,
                _ => return conflict(resolution),
            };
            let same = coordinator == resolution.coordinator.original_resolution_identity()
                || resolution
                    .node
                    .as_ref()
                    .is_some_and(|node| coordinator == node.original_resolution_identity());
            if !plain || !same {
                return conflict(resolution);
            }
            {
                let mut selected = resolution
                    .selection
                    .lock()
                    .expect("resolution selection poisoned");
                match &*selected {
                    Some((original, original_scope))
                        if original != domain || *original_scope != scope =>
                    {
                        return conflict(resolution)
                    }
                    None => *selected = Some((domain.to_owned(), scope)),
                    _ => {}
                }
            }
            let mut selected = resolution.calls[index]
                .0
                .registration
                .lock()
                .expect("original DNS caller poisoned");
            match &*selected {
                Some(original) if !Arc::ptr_eq(original, registration) => conflict(resolution),
                Some(_) => Ok(()),
                None => {
                    *selected = Some(registration.clone());
                    Ok(())
                }
            }
        })
        .unwrap_or(Ok(()))
}
fn conflict<T>(resolution: &Resolution) -> io::Result<T> {
    resolution.conflict.store(true, Ordering::Release);
    Err(io::Error::other(
        "DNS resolution caller changed its actual domain/purpose/generation or original flight",
    ))
}

#[cfg(test)]
mod tests;
