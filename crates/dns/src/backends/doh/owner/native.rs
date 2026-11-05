//! Original Scope registration, native loans and leases are separate gates.
use super::*;
use crate::OwnedDnsQueryTasks;
impl DriverOwner {
    pub(in super::super) fn select_host(&self, tasks: OwnedDnsQueryTasks) -> io::Result<()> {
        tasks.ensure_open()?;
        self.ensure_open()?;
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        match &state.mode {
            PoolMode::Unused => state.mode = PoolMode::Host(tasks),
            PoolMode::Host(current) if current.same_owner(&tasks) => {}
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "DoH pool has another original owner",
                ))
            }
        }
        Ok(())
    }
    pub(in super::super) fn spawn_native(
        &self,
        tasks: &OwnedDnsQueryTasks,
        client: Arc<ClientState>,
        network: u64,
        job: impl Future<Output = ()> + Send + 'static,
    ) -> io::Result<()> {
        let _callbacks = tasks.defer_callbacks();
        let mut state = self.state.lock().expect("DoH driver owner poisoned");
        state.reap();
        if state.closing
            || self.cancel.is_cancelled()
            || !matches!(&state.mode, PoolMode::Host(current) if current.same_owner(tasks))
        {
            return Err(io::Error::new(
                io::ErrorKind::NotConnected,
                "DoH original host admission retired",
            ));
        }
        if state.native.len() >= self.limit {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "DoH native endpoint driver capacity exhausted",
            ));
        }
        state.native.push(client.clone());
        match tasks.spawn_driver(job, client.cancel.clone(), network) {
            Ok(witness) => {
                client
                    .native
                    .get()
                    .expect("retained original before spawn")
                    .task
                    .set(witness)
                    .expect("original task bound once");
                self.changed.notify_waiters();
                Ok(())
            }
            Err(source) => {
                state.native.retain(|entry| !Arc::ptr_eq(entry, &client));
                Err(source)
            }
        }
    }
    pub(super) async fn drain_native(&self, end: Instant) -> io::Result<()> {
        let entries = {
            let mut state = self.state.lock().expect("DoH driver owner poisoned");
            state.reap();
            state
                .native
                .iter()
                .filter(|client| client.cancel.is_cancelled())
                .cloned()
                .collect::<Vec<_>>()
        };
        let mut failure = None;
        for client in entries {
            let idle = async {
                loop {
                    let changed = self.changed.notified();
                    tokio::pin!(changed);
                    changed.as_mut().enable();
                    if client.leases.load(std::sync::atomic::Ordering::Acquire) == 0 {
                        break;
                    }
                    changed.await;
                }
            };
            let settled = async {
                tokio::time::timeout_at(end, idle)
                    .await
                    .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                client
                    .native
                    .get()
                    .expect("native original retained")
                    .settle(end)
                    .await
            }
            .await;
            if let Err(source) = settled {
                failure.get_or_insert(source);
            }
        }
        self.state.lock().expect("DoH driver owner poisoned").reap();
        failure.map_or(Ok(()), Err)
    }
}
