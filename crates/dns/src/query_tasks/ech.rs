//! Explicit ECH endpoint facts retained by the actual query/physical driver.
use super::*;
impl OwnedDnsQueryTasks {
    pub(crate) fn bind_ech_endpoint(endpoint: &crate::EchDnsEndpoint) -> io::Result<()> {
        QUERY_RECORD
            .try_with(|record| {
                let mut target = record.ech.lock().expect("DNS ECH target poisoned");
                if target.as_ref().is_some_and(|current| current != endpoint) {
                    return Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        "actual DNS job already names another ECH endpoint",
                    ));
                }
                *target = Some(endpoint.clone());
                Ok(())
            })
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "ECH has no actual query registration",
                )
            })?
    }
    /// Read-only endpoint of this exact actual registration. The provider must
    /// still match a cold declaration and its private native grants.
    pub fn ech_endpoint_for(&self, caller: &TaskJoinWitness) -> Option<crate::EchDnsEndpoint> {
        self.0
            .state
            .lock()
            .expect("DNS query tasks poisoned")
            .records
            .iter()
            .find_map(|record| {
                let actual = record.registration.get()?.as_ref()?;
                if !actual.witness.same_registration(caller) || caller.is_joined() {
                    return None;
                }
                record.ech.lock().expect("DNS ECH target poisoned").clone()
            })
    }
}
impl OwnedDnsQueryTasks {
    pub(crate) fn spawn_ech_job(
        &self,
        job: impl Future<Output = ()> + Send + 'static,
        cancel: CancellationToken,
        network: u64,
    ) -> io::Result<TaskJoinWitness> {
        // The only cross-purpose entry is this concrete Node ECH operation;
        // inherited_tasks still requires the actual same-budget parent record.
        BOOTSTRAP_TARGET.sync_scope(self.0.downgrade(), || {
            self.spawn_kind(job, cancel, Some(network), TaskKind::Query)
        })
    }
}
