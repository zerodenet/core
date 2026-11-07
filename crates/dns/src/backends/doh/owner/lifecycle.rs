//! Retire the one actual pool before admitting a replacement.
use super::*;
impl DriverOwner {
    /// Marks the real pool unavailable synchronously. The owner remains in the
    /// DNS registry until shutdown has joined every driver and affected user.
    pub(crate) fn retire(&self) {
        let clients = {
            let mut state = self.state.lock().expect("DoH driver owner poisoned");
            state.closing = true;
            state
                .records
                .values()
                .chain(state.native.iter())
                .cloned()
                .collect::<Vec<_>>()
        };
        self.cancel.cancel();
        for client in clients {
            client.retire();
        }
        self.changed.notify_waiters();
    }
    /// Passive positive inventory/receiver observation. Registry/cold-cache
    /// callers may hold their own locks, so this never consumes exits or wakes.
    pub(crate) fn drained(&self) -> bool {
        let physically_retired = {
            let state = self.state.lock().expect("DoH driver owner poisoned");
            state.tasks.is_empty() && state.users.is_empty() && state.native.is_empty()
        };
        physically_retired && self.retained_failures_received()
    }
    /// Reaps actual exits during ordinary replacement, before a new socket is
    /// opened. Unchanged, usable endpoint slots are left running.
    pub(in super::super) async fn drain_retired(&self, deadline: Instant) -> io::Result<()> {
        loop {
            self.drain_native(deadline).await?;
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            self.reap();
            let next = std::future::poll_fn(|cx| {
                let mut state = self.state.lock().expect("DoH driver owner poisoned");
                let retired = state
                    .records
                    .values()
                    .any(|client| client.cancel.is_cancelled());
                if !retired && state.users.is_empty() {
                    return Poll::Ready(());
                }
                match state.tasks.poll_join_next_with_id(cx) {
                    Poll::Ready(Some(result)) => {
                        let client = state.exit(result);
                        drop(state);
                        client.retire();
                        Poll::Ready(())
                    }
                    _ => Poll::Pending,
                }
            });
            tokio::time::timeout_at(deadline, async {
                tokio::select! {biased;_=next=>{},_=&mut changed=>{}}
            })
            .await
            .map_err(|source| crate::retirement_observation::deadline(source, ""))?;
            self.reap();
            let state = self.state.lock().expect("DoH driver owner poisoned");
            if !state
                .records
                .values()
                .any(|client| client.cancel.is_cancelled())
                && state.users.is_empty()
                && !state
                    .native
                    .iter()
                    .any(|client| client.cancel.is_cancelled())
            {
                let failure = state.failure.as_ref().map(|source| faults::Retained {
                    first: source.clone(),
                    hosts: state.host_failures.clone(),
                });
                drop(state);
                return match failure {
                    Some(source) if !self.retained_failures_received() => {
                        Err(io::Error::other(source))
                    }
                    _ => Ok(()),
                };
            }
        }
    }
    pub(crate) async fn shutdown(&self, deadline: Instant) -> io::Result<()> {
        self.retire();
        // The serialized open gate also covers inline TCP/TLS/H2 setup,
        // before a connection driver can be registered.
        let _opening = tokio::time::timeout_at(deadline, self.connect_lock.lock())
            .await
            .map_err(|source| crate::retirement_observation::deadline(source, ""))?;
        let clients = {
            let mut clients = tokio::time::timeout_at(deadline, self.clients.lock())
                .await
                .map_err(|source| crate::retirement_observation::deadline(source, ""))?;
            std::mem::take(&mut *clients)
        };
        drop(clients);
        let result = self.drain_retired(deadline).await;
        if let Some(source) = &self
            .state
            .lock()
            .expect("DoH driver owner poisoned")
            .transport_error
        {
            tracing::debug!(%source,"observed DoH transport failure before confirmed pool retirement");
        }
        result
    }
}
