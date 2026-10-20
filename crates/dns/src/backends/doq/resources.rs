//! The actual endpoints belonging to one backend, independently of other
//! configured DoQ backends. Strong task ownership remains in TransportOwners.
use super::owner::EndpointOwner;
use std::{
    io,
    sync::{Arc, Mutex, Weak},
};

#[derive(Default)]
pub(super) struct EndpointSet(Mutex<State>);
#[derive(Default)]
struct State {
    closing: bool,
    owners: Vec<Weak<EndpointOwner>>,
}
impl EndpointSet {
    pub(super) fn register(
        &self,
        owner: Arc<EndpointOwner>,
        registry: &crate::transport_owners::TransportOwners,
    ) -> io::Result<()> {
        let mut state = self.0.lock().expect("DoQ backend resources poisoned");
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
            .0
            .lock()
            .expect("DoQ backend resources poisoned")
            .closing
    }
    pub(super) fn retire(&self) {
        let mut state = self.0.lock().expect("DoQ backend resources poisoned");
        state.closing = true;
        for owner in state.owners.iter().filter_map(Weak::upgrade) {
            owner.request_close();
        }
    }
    pub(super) async fn shutdown(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        self.retire();
        let owners = self
            .0
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
        failure.map_or(Ok(()), Err)
    }
}
