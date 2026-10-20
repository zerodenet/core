//! The actual endpoints belonging to one backend, independently of other
//! configured DoQ backends. Strong task ownership remains in TransportOwners.
use super::owner::EndpointOwner;
use std::{
    io,
    sync::{Arc, Mutex, Weak},
};

#[derive(Default)]
pub(super) struct EndpointSet {
    state: Mutex<State>,
    cancel: zero_platform_tokio::contract::CancellationToken,
    changed: tokio::sync::Notify,
}
#[derive(Default)]
struct State {
    closing: bool,
    owners: Vec<Weak<EndpointOwner>>,
    query_users: usize,
}
impl EndpointSet {
    pub(super) fn register(
        &self,
        owner: Arc<EndpointOwner>,
        registry: &crate::transport_owners::TransportOwners,
    ) -> io::Result<()> {
        let mut state = self.state.lock().expect("DoQ backend resources poisoned");
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoQ backend is retired",
            ));
        }
        registry.register_doq(owner.clone())?;
        state.owners.retain(|owner| owner.strong_count() > 0);
        state.owners.push(Arc::downgrade(&owner));
        Ok(())
    }
    pub(super) fn open(&self) -> bool {
        !self
            .state
            .lock()
            .expect("DoQ backend resources poisoned")
            .closing
    }
    pub(super) fn retire(&self) {
        let mut state = self.state.lock().expect("DoQ backend resources poisoned");
        state.closing = true;
        let owners = state
            .owners
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        drop(state);
        self.cancel.cancel();
        for owner in owners {
            owner.request_close();
        }
    }
    pub(super) async fn shutdown(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        self.retire();
        let owners = self
            .state
            .lock()
            .expect("DoQ backend resources poisoned")
            .owners
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        let mut failure = None;
        for owner in owners {
            if let Err(source) = owner.shutdown(deadline).await {
                failure.get_or_insert(source);
            }
        }
        if let Err(source) = self.wait_queries(deadline).await {
            failure.get_or_insert(source);
        }
        failure.map_or(Ok(()), Err)
    }
}

pub(super) struct QueryUser<'a>(&'a EndpointSet);
impl EndpointSet {
    pub(super) fn query_user(&self) -> io::Result<QueryUser<'_>> {
        let mut state = self.state.lock().expect("DoQ backend resources poisoned");
        if state.closing {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoQ backend is retired",
            ));
        }
        state.query_users = state
            .query_users
            .checked_add(1)
            .ok_or_else(|| io::Error::other("DoQ query identity capacity exhausted"))?;
        Ok(QueryUser(self))
    }
    async fn wait_queries(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self
                .state
                .lock()
                .expect("DoQ backend resources poisoned")
                .query_users
                == 0
            {
                return Ok(());
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        }
    }
}
impl QueryUser<'_> {
    pub(super) fn cancellation(&self) -> zero_platform_tokio::contract::CancellationToken {
        self.0.cancel.child_token()
    }
}
impl Drop for QueryUser<'_> {
    fn drop(&mut self) {
        self.0
            .state
            .lock()
            .expect("DoQ backend resources poisoned")
            .query_users -= 1;
        self.0.changed.notify_waiters();
    }
}
