//! OS work is borrowed; completion is the real blocking child's Scope join,
//! never a fabricated receipt for the OS resolver's private sockets.
use super::*;
use std::net::ToSocketAddrs;
use tokio::time::Instant;
use zero_platform_tokio::contract::scope::TaskJoined;

pub struct NativeSystemLookup {
    pub(crate) result: io::Result<Vec<zero_traits::IpAddress>>,
    pub(crate) joined: TaskJoined,
}
impl OwnedDnsQueryTasks {
    pub async fn lookup_system(
        &self,
        name: String,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: Instant,
    ) -> io::Result<NativeSystemLookup> {
        self.lookup_system_with(caller, cancel, end, move || {
            (name.as_str(), 0).to_socket_addrs().map(|addresses| {
                addresses
                    .map(|address| match address.ip() {
                        std::net::IpAddr::V4(ip) => zero_traits::IpAddress::V4(ip.octets()),
                        std::net::IpAddr::V6(ip) => zero_traits::IpAddress::V6(ip.octets()),
                    })
                    .collect()
            })
        })
        .await
    }
    pub(super) async fn lookup_system_with(
        &self,
        caller: TaskJoinWitness,
        cancel: CancellationToken,
        end: Instant,
        work: impl FnOnce() -> io::Result<Vec<zero_traits::IpAddress>> + Send + 'static,
    ) -> io::Result<NativeSystemLookup> {
        self.ensure_open()?;
        if !caller
            .scope_identity()
            .same_scope(&self.0.registrar.scope_identity())
            || caller.is_joined()
            || cancel.is_cancelled()
            || Instant::now() >= end
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "system lookup requires its active original caller",
            ));
        }
        let parent = self
            .0
            .state
            .lock()
            .expect("DNS query tasks poisoned")
            .records
            .iter()
            .find(|record| {
                record
                    .registration
                    .get()
                    .and_then(Option::as_ref)
                    .is_some_and(|registration| registration.witness.same_registration(&caller))
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "system caller is not an original DNS job",
                )
            })?
            .clone();
        let network = parent.network.get().copied();
        let ancestors = self.ancestors_of(&parent, &caller)?;
        let record = Arc::new(Record {
            budget: self.0.budget.clone(),
            capacity: Mutex::new(Some(self.0.budget.reserve(TaskKind::Blocking)?)),
            kind: TaskKind::Blocking,
            ancestors,
            network: network.map(OnceLock::from).unwrap_or_default(),
            registration: OnceLock::new(),
            bound: Notify::new(),
            cancel,
            io_pending: Default::default(),
            original: Mutex::new(None),
            metadata: Mutex::new(None),
        });
        self.0
            .budget
            .retained
            .lock()
            .expect("DNS budget poisoned")
            .push(Arc::downgrade(&record));
        {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            if state.closing || self.0.parent.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "DNS lookup admission retired",
                ));
            }
            if state
                .records
                .iter()
                .filter(|record| record.kind == TaskKind::Blocking)
                .count()
                >= self.0.limit.get()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "system DNS lookup capacity exhausted",
                ));
            }
            state.records.try_reserve(1).map_err(io::Error::other)?;
            state.records.push(record.clone());
        }
        let (bound, begin) = std::sync::mpsc::sync_channel(1);
        let (completed, result) = oneshot::channel();
        let callbacks = self.0.registrar.defer_callbacks();
        let spawned = self
            .0
            .registrar
            .spawn_blocking_tracked(self.0.class, move || {
                if begin.recv().is_err() {
                    return Ok(());
                }
                let addresses = work();
                // A cancelled caller returns custody, not proof of OS completion.
                let _ = completed.send(addresses);
                Ok(())
            });
        let (id, witness) = match spawned {
            Ok(spawned) => spawned,
            Err(source) => {
                record
                    .registration
                    .set(None)
                    .expect("failed lookup bound once");
                record.bound.notify_waiters();
                self.0
                    .state
                    .lock()
                    .expect("DNS query tasks poisoned")
                    .records
                    .retain(|current| !Arc::ptr_eq(current, &record));
                drop(callbacks);
                return Err(io::Error::other(source));
            }
        };
        record
            .registration
            .set(Some(QueryRegistration {
                id,
                witness: witness.clone(),
            }))
            .expect("system original bound once");
        record.bound.notify_waiters();
        let _ = bound.send(());
        drop(callbacks);
        let addresses = tokio::time::timeout_at(end, result)
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
            .map_err(io::Error::other)?;
        let joined = tokio::time::timeout_at(end, witness.wait())
            .await
            .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
        if !witness.matches_joined(&joined) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "system lookup original join mismatch",
            ));
        }
        joined.result().map_err(io::Error::other)?;
        Ok(NativeSystemLookup {
            result: addresses,
            joined,
        })
    }
}
