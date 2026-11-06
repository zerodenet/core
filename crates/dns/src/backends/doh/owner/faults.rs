//! Historical errors have a real bounded receiver, separate from IO retirement.
use super::*;
use zero_platform_tokio::contract::{ChannelError, scope::TaskJoinWitness};
#[derive(Clone, Debug)]
pub(super) struct HostFailure {
    pub(super) task: TaskJoinWitness,
    pub(super) error: Arc<ChannelError>,
}
#[derive(Debug)]
pub(super) struct Retained {
    pub(super) first: Arc<io::Error>,
    // The returned fault keeps every actual original even before its canonical
    // receiver finishes acceptance. No task ID, receipt or error is invented.
    pub(super) hosts: Vec<HostFailure>,
}
impl std::fmt::Display for Retained {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({} original Host driver faults retained)",
            self.first,
            self.hosts.len()
        )
    }
}
impl std::error::Error for Retained {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.hosts
            .first()
            .map(|fault| fault.error.as_ref() as &(dyn std::error::Error + 'static))
            .or(Some(self.first.as_ref()))
    }
}
impl DriverOwner {
    pub(super) fn retained_failures_received(&self) -> bool {
        let (tasks, faults) = {
            let state = self.state.lock().expect("DoH driver owner poisoned");
            if state.standalone_failure {
                return false;
            }
            if state.failure.is_none() {
                return true;
            }
            let PoolMode::Host(tasks) = &state.mode else {
                return false;
            };
            if state.host_failures.is_empty() {
                return false;
            }
            (tasks.clone(), state.host_failures.clone())
        };
        // Canonical QueryFaults strongly retains the real witness and exact
        // typed source until the original Slot CloseReport handoff. Local
        // acceptance alone is insufficient: the actual Host must also ACK.
        faults.iter().all(|fault| {
            let Some(joined) = fault.task.try_joined() else {
                return false;
            };
            fault.task.matches_joined(&joined)
                && joined.failure_accepted()
                && joined.registration_released()
                && joined
                    .result()
                    .err()
                    .is_some_and(|source| Arc::ptr_eq(&source, &fault.error))
                && tasks.retains_query_failure(&fault.task, None)
        })
    }
}
