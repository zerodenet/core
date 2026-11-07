//! Original Query/Blocking retirement for an actual DNS profile transition.
use super::*;
impl OwnedDnsQueryTasks {
    pub(super) async fn retire_paused_queries(&self, end: tokio::time::Instant) -> io::Result<()> {
        loop {
            self.reap();
            let records = {
                let state = self.0.state.lock().expect("DNS query tasks poisoned");
                if state.pauses == 0 || state.identity_fault {
                    return Err(io::Error::other(
                        "actual DNS query retirement lost its admission fence",
                    ));
                }
                state
                    .records
                    .iter()
                    .filter(|record| !physical_driver(record.kind))
                    .cloned()
                    .collect::<Vec<_>>()
            };
            for record in &records {
                record.cancel.cancel();
            }
            for record in records {
                let bound = async {
                    loop {
                        let changed = record.bound.notified();
                        tokio::pin!(changed);
                        changed.as_mut().enable();
                        if let Some(registration) = record.registration.get() {
                            return registration.as_ref();
                        }
                        changed.await;
                    }
                };
                if let Some(registration) = tokio::time::timeout_at(end, bound)
                    .await
                    .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
                {
                    let joined = tokio::time::timeout_at(end, registration.witness.wait())
                        .await
                        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                    if !registration.witness.matches_joined(&joined) {
                        return Err(io::Error::other(
                            "DNS profile retirement observed a foreign query exit",
                        ));
                    }
                    // Persist/locally accept this exact original failed exit
                    // promptly; the real Host can then ACK while siblings join.
                    self.reap();
                }
            }
            self.reap();
            let originals = self
                .0
                .budget
                .retained
                .lock()
                .expect("DNS budget poisoned")
                .clone();
            let scope = self.0.registrar.scope_identity();
            for original in originals {
                if physical_driver(original.kind) {
                    continue;
                }
                let relevant = match original.registration.get() {
                    Some(Some(actual)) => actual.witness.scope_identity().same_scope(&scope),
                    Some(None) => false, // Actual failed registration, before IO.
                    None => original.finalizer_matches(self),
                };
                if !relevant {
                    continue;
                }
                let Some(Some(actual)) = original.registration.get() else {
                    return Err(io::Error::other(
                        "original DNS profile query registration remains unknown",
                    ));
                };
                let joined =
                    tokio::time::timeout_at(end, actual.witness.wait_registration_released())
                        .await
                        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?;
                if !actual.witness.matches_joined(&joined) || !original.native_complete() {
                    return Err(io::Error::other(
                        "original DNS profile query native custody remains unsettled",
                    ));
                }
            }
            let has_queries = self
                .0
                .state
                .lock()
                .expect("DNS query tasks poisoned")
                .records
                .iter()
                .any(|record| !physical_driver(record.kind));
            if !has_queries {
                return self.confirm_failure_reception();
            }
            // A task may have installed an original blocking child immediately
            // before the fence. Parent join ends that race; rescan the same book.
        }
    }
}
