//! Consume actual Query/Blocking/Driver joins without discarding retained debt.
use super::*;
impl OwnedDnsQueryTasks {
    pub(super) fn reap(&self) {
        loop {
            let (retired, failed) = {
                let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
                let ready = state
                    .records
                    .iter()
                    .enumerate()
                    .find_map(|(index, record)| {
                        let bound = record.registration.get()?;
                        match bound {
                            None => Some((index, None)),
                            Some(registration) => registration
                                .witness
                                .try_joined()
                                .map(|joined| (index, Some((registration.id, joined)))),
                        }
                    });
                let Some((index, joined)) = ready else { return };
                let mut failed = None;
                if let Some((id, joined)) = joined {
                    if joined.id() != id {
                        state.closing = true;
                        state.identity_fault = true;
                        state.first_failure.get_or_insert_with(|| {
                            Arc::new(ChannelError::new(
                                zero_platform_tokio::contract::Stage::Close,
                                zero_platform_tokio::contract::Reason::Internal,
                                zero_platform_tokio::contract::Impact::Component,
                            ))
                        });
                        return; // Keep the unmatched original record charged.
                    }
                    if let Err(source) = joined.result() {
                        state.first_failure.get_or_insert(source);
                        failed = Some(joined);
                    }
                }
                (state.records.remove(index), failed)
            };
            let _ = retired.retirement.release_joined();
            if let Some(joined) = failed {
                let Some(actual) = retired.registration.get().and_then(Option::as_ref) else {
                    return;
                };
                if self
                    .retain_query_failure(&retired.retirement, &actual.witness)
                    .is_err()
                    || !actual.witness.matches_joined(&joined)
                {
                    self.0
                        .state
                        .lock()
                        .expect("DNS query tasks poisoned")
                        .identity_fault = true;
                    return;
                }
            }
            // Actual witness/cancel/Notify destruction can invoke an external
            // waker; no final retired Record drop runs under the enclosing lock.
            drop(retired);
        }
    }
    pub(crate) async fn next_exit(&self) -> bool {
        self.reap();
        let record = {
            let state = self.0.state.lock().expect("DNS query tasks poisoned");
            if state.identity_fault {
                return false;
            }
            state.records.first().cloned()
        };
        let Some(record) = record else { return false };
        loop {
            let bound = record.bound.notified();
            tokio::pin!(bound);
            bound.as_mut().enable();
            if let Some(bound) = record.registration.get() {
                if let Some(registration) = bound {
                    registration.witness.wait().await;
                }
                self.reap();
                return true;
            }
            bound.await;
        }
    }
    /// Closes query admission and waits for actual Host Scope consumption.
    /// Does not drive that Scope, close any socket or confirm resolver users.
    pub(crate) async fn shutdown(&self, deadline: tokio::time::Instant) -> io::Result<()> {
        let records = {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            state.closing = true;
            state.records.clone()
        };
        for record in records {
            record.cancel.cancel();
        }
        while tokio::time::timeout_at(deadline, self.next_exit())
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "DNS original Host Scope query join is unconfirmed",
                )
            })?
        {}
        let state = self.0.state.lock().expect("DNS query tasks poisoned");
        match &state.first_failure {
            Some(source) => Err(io::Error::other(source.clone())),
            None => Ok(()),
        }
    }
}
