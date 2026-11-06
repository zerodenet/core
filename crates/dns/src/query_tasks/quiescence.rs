//! Reversible admission pause, never cancellation, native closure or task ACK.
use super::*;
#[must_use]
pub(crate) struct QueryPause(OwnedDnsQueryTasks);
impl QueryPause {
    pub(crate) fn same_owner(&self, tasks: &OwnedDnsQueryTasks) -> bool {
        self.0.same_owner(tasks)
    }
}
impl Drop for QueryPause {
    fn drop(&mut self) {
        let mut state = self.0.0.state.lock().expect("DNS query tasks poisoned");
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
        self.reap();
        {
            // This is the same lock as final original Query/Blocking/Driver
            // registration. A call that passed ensure_open cannot cross it.
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            if let Some(error) = &state.first_failure {
                return Err(io::Error::other(error.clone()));
            }
            if state.closing || state.identity_fault || self.0.parent.is_cancelled() {
                return Err(io::Error::new(
                    io::ErrorKind::NotConnected,
                    "original DNS owner is retiring",
                ));
            }
            if state
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
        let guard = QueryPause(self.clone());
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
