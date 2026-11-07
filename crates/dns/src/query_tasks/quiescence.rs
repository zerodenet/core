//! Reversible registration fencing; a normal profile transition separately
//! cancels original queries and observes their real native/join/ACK retirement.
use super::*;
#[must_use]
pub(crate) struct QueryPause(OwnedDnsQueryTasks, bool);
impl QueryPause {
    pub(crate) fn cancel_queries(&self) -> io::Result<()> {
        if !self.1 {
            return Ok(());
        }
        let records = {
            let state = self.0 .0.state.lock().expect("DNS query tasks poisoned");
            if state.pauses == 0 {
                return Err(io::Error::other(
                    "actual DNS query admission fence was lost",
                ));
            }
            state
                .records
                .iter()
                .filter(|record| !physical_driver(record.kind))
                .cloned()
                .collect::<Vec<_>>()
        };
        for record in records {
            record.cancel.cancel();
        }
        Ok(())
    }
    pub(crate) async fn settle_queries(&self, end: tokio::time::Instant) -> io::Result<()> {
        if self.1 {
            self.0.retire_paused_queries(end).await
        } else {
            Ok(())
        }
    }
}
impl Drop for QueryPause {
    fn drop(&mut self) {
        let mut state = self.0 .0.state.lock().expect("DNS query tasks poisoned");
        state.pauses = state
            .pauses
            .checked_sub(1)
            .expect("actual query pause retained");
    }
}
fn physical_driver(kind: TaskKind) -> bool {
    #[cfg(feature = "doh")]
    {
        kind == TaskKind::Driver
    }
    #[cfg(not(feature = "doh"))]
    {
        let _ = kind;
        false
    }
}
impl OwnedDnsQueryTasks {
    pub(crate) fn try_pause_queries(&self) -> io::Result<QueryPause> {
        self.pause_queries(false)
    }
    pub(crate) fn pause_queries_for_profile_change(&self) -> io::Result<QueryPause> {
        self.pause_queries(true)
    }
    fn pause_queries(&self, cancel: bool) -> io::Result<QueryPause> {
        self.reap();
        {
            // This is the same lock as final original Query/Blocking/Driver
            // registration. A call that passed ensure_open cannot cross it.
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            if let Some(error) = &state.first_failure {
                if !cancel || state.identity_fault {
                    return Err(io::Error::other(error.clone()));
                }
            }
            if state.closing || state.identity_fault || self.0.parent.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "original DNS owner is retiring",
                ));
            }
            if !cancel
                && state
                    .records
                    .iter()
                    .any(|record| !physical_driver(record.kind))
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "actual DNS query or blocking worker remains live",
                ));
            }
            state.pauses = state
                .pauses
                .checked_add(1)
                .ok_or_else(|| io::Error::other("query pause count exhausted"))?;
        }
        let guard = QueryPause(self.clone(), cancel);
        if cancel {
            return Ok(guard);
        }
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
                Some(None) => false, // Actual original registration failed before submission.
                None => original.finalizer_matches(self),
            };
            if !relevant {
                continue;
            }
            let joined = original
                .registration
                .get()
                .and_then(Option::as_ref)
                .and_then(|actual| actual.witness.try_joined());
            if !joined.is_some_and(|joined| joined.registration_released())
                || !original.native_complete()
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "original DNS query cleanup or registration ACK remains retained",
                ));
            }
        }
        Ok(guard)
    }
}

mod drain;
