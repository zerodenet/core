//! Persistent DNS carrier drivers share the actual query owner's Scope.
use super::*;
impl OwnedDnsQueryTasks {
    pub(crate) fn defer_callbacks(
        &self,
    ) -> Option<zero_platform_tokio::contract::scope::HostedCallbacks> {
        self.0.registrar.defer_callbacks()
    }

    pub(crate) fn spawn_driver(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
        network: u64,
    ) -> io::Result<TaskJoinWitness> {
        self.spawn_kind(job, cancel, Some(network), TaskKind::Driver)
    }
}
