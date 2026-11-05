//! A network transition retains all old original jobs, including blocking OS
//! work, until the real Scope joins. It issues no native-close evidence.
use super::*;
impl OwnedDnsQueryTasks {
    pub async fn fence_network(
        &self,
        generation: u64,
        caller: &TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> io::Result<()> {
        self.ensure_open()?;
        loop {
            let originals = {
                let state = self.0.state.lock().expect("DNS query tasks poisoned");
                let current = state
                    .records
                    .iter()
                    .find(|record| {
                        record
                            .registration
                            .get()
                            .and_then(Option::as_ref)
                            .is_some_and(|registration| {
                                registration.witness.same_registration(caller)
                            })
                    })
                    .ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::PermissionDenied,
                            "network fence requires the actual DNS job",
                        )
                    })?;
                if current.network.get().is_some_and(|old| *old != generation) {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "old DNS query cannot cross its original network",
                    ));
                }
                let _ = current.network.set(generation);
                state
                    .records
                    .iter()
                    .filter(|record| record.network.get().is_some_and(|old| *old != generation))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            if originals.is_empty() {
                self.reap();
                return self.ensure_open();
            }
            for original in &originals {
                original.cancel.cancel();
            }
            for original in originals {
                loop {
                    let bound = original.bound.notified();
                    tokio::pin!(bound);
                    bound.as_mut().enable();
                    if let Some(registration) = original.registration.get() {
                        if let Some(registration) = registration {
                            tokio::time::timeout_at(end, registration.witness.wait())
                                .await
                                .map_err(|source| {
                                    io::Error::new(io::ErrorKind::TimedOut, source)
                                })?;
                        }
                        break;
                    }
                    tokio::time::timeout_at(end, bound)
                        .await
                        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                }
            }
            // An old query may have installed a blocking child after the first
            // snapshot. Joining the parent ends that race; rescan the same ledger
            // before allowing any acquisition in the new network.
            self.reap();
            self.ensure_open()?;
        }
    }
}
