//! The canonical query owner receives real failed registrations before local ACK.
use super::*;
#[derive(Debug)]
pub struct OwnedDnsQueryFault {
    pub query: TaskJoinWitness,
    pub joined: zero_platform_tokio::contract::scope::TaskJoined,
    pub error: Arc<ChannelError>,
    pub cleanup_failure: Option<Arc<io::Error>>,
}
#[derive(Debug)]
pub struct OwnedDnsQueryFaults(Box<[OwnedDnsQueryFault]>);
impl OwnedDnsQueryFaults {
    pub fn faults(&self) -> &[OwnedDnsQueryFault] {
        &self.0
    }
}
impl std::fmt::Display for OwnedDnsQueryFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} original DNS worker/cleanup failures", self.0.len())
    }
}
impl std::error::Error for OwnedDnsQueryFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0
            .first()
            .map(|fault| fault.error.as_ref() as &(dyn std::error::Error + 'static))
    }
}
impl OwnedDnsQueryTasks {
    pub(crate) fn retain_query_failure(
        &self,
        original: &Arc<Retirement>,
        query: &TaskJoinWitness,
    ) -> io::Result<()> {
        let joined = query
            .try_joined()
            .ok_or_else(|| io::Error::other("DNS failed worker is unjoined"))?;
        let error = joined.result().map_or_else(Ok, |_| {
            Err(io::Error::other(
                "successful query cannot invent a task failure",
            ))
        })?;
        let actual = original
            .registration
            .get()
            .and_then(Option::as_ref)
            .ok_or_else(|| io::Error::other("DNS original registration is unknown"))?;
        if !actual.witness.same_registration(query)
            || !query.matches_joined(&joined)
            || !query
                .scope_identity()
                .same_scope(&self.0.registrar.scope_identity())
        {
            return Err(io::Error::other(
                "DNS failure names a foreign original registration",
            ));
        }
        {
            let mut state = self.0.state.lock().expect("DNS query tasks poisoned");
            // Fatal ownership/worker failures alone stop new work. Ordinary
            // upstream errors stay with their real Flight/caller and job Ok.
            state.first_failure.get_or_insert(error);
            if let Some(existing) = state.faults.get(&actual.id) {
                if !Arc::ptr_eq(existing, original) {
                    state.identity_fault = true;
                    return Err(io::Error::other(
                        "DNS failed registration conflicts with its actual receiver",
                    ));
                }
            } else {
                state.faults.insert(actual.id, original.clone());
            }
        }
        // The bounded canonical map now strongly stores this same witness,
        // its original joined result/error Arc and its cleanup source. This
        // accepts local delivery only; it neither ACKs the Host nor frees IO.
        query
            .acknowledge_hosted_failure(&joined)
            .map_err(io::Error::other)
    }
    pub(crate) fn retain_cleanup_failure(
        &self,
        original: &Arc<Retirement>,
        query: &TaskJoinWitness,
        source: io::Error,
    ) -> io::Result<Arc<io::Error>> {
        // A successful job cannot fabricate a failed registration merely to
        // publish a later cleanup error. Its actual caller must keep that fault.
        if query
            .try_joined()
            .is_none_or(|joined| joined.result().is_ok())
        {
            return Err(source);
        }
        let source = original.retain_cleanup_failure(source);
        self.retain_query_failure(original, query)?;
        Ok(source)
    }
    pub(crate) fn retains_query_failure(
        &self,
        query: &TaskJoinWitness,
        cleanup: Option<&Arc<io::Error>>,
    ) -> bool {
        let Some(id) = query.task_id() else {
            return false;
        };
        let original = self
            .0
            .state
            .lock()
            .expect("DNS query tasks poisoned")
            .faults
            .get(&id)
            .cloned();
        original.is_some_and(|original| {
            original
                .registration
                .get()
                .and_then(Option::as_ref)
                .is_some_and(|actual| actual.witness.same_registration(query))
                && cleanup.is_none_or(|source| {
                    original
                        .cleanup_failure()
                        .is_some_and(|actual| Arc::ptr_eq(&actual, source))
                })
        })
    }
    /// Confirmation of this existing receiver only, not native cleanup or
    /// permission to reopen a failed query owner. Ordinary work admission
    /// continues to see first_failure until that actual owner is replaced.
    pub(super) fn confirm_failure_reception(&self) -> io::Result<()> {
        let (first, unknown, originals) = {
            let state = self.0.state.lock().expect("DNS query tasks poisoned");
            (
                state.first_failure.clone(),
                state.identity_fault,
                state
                    .faults
                    .iter()
                    .map(|(id, original)| (*id, original.clone()))
                    .collect::<Vec<_>>(),
            )
        };
        if unknown {
            return Err(first.map_or_else(
                || {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        "DNS original fault identity is unknown",
                    )
                },
                io::Error::other,
            ));
        }
        let Some(first) = first else {
            return Ok(());
        };
        let mut first_received = false;
        for (id, original) in originals {
            let Some(actual) = original.registration.get().and_then(Option::as_ref) else {
                return Err(io::Error::other(first));
            };
            let Some(joined) = actual.witness.try_joined() else {
                return Err(io::Error::other(first));
            };
            let Err(source) = joined.result() else {
                return Err(io::Error::other(first));
            };
            if actual.id != id
                || joined.id() != id
                || !actual.witness.matches_joined(&joined)
                || !joined.failure_accepted()
                || !joined.registration_released()
                || !actual
                    .witness
                    .scope_identity()
                    .same_scope(&self.0.registrar.scope_identity())
            {
                return Err(io::Error::other(source));
            }
            first_received |= Arc::ptr_eq(&source, &first);
        }
        if !first_received {
            return Err(io::Error::other(first));
        }
        Ok(())
    }
    /// Actual finite fault receiver. The first failure closes this Scope's
    /// query admission, so rows are bounded by its already registered tasks.
    /// Records remain until this owner's lifecycle hands them to its host.
    pub fn retained_faults(&self) -> Option<Arc<OwnedDnsQueryFaults>> {
        let originals = self
            .0
            .state
            .lock()
            .expect("DNS query tasks poisoned")
            .faults
            .values()
            .cloned()
            .collect::<Vec<_>>();
        let faults = originals
            .into_iter()
            .filter_map(|original| {
                let query = original.registration.get()?.as_ref()?.witness.clone();
                let joined = query.try_joined()?;
                let error = joined.result().err()?;
                Some(OwnedDnsQueryFault {
                    query,
                    joined,
                    error,
                    cleanup_failure: original.cleanup_failure(),
                })
            })
            .collect::<Vec<_>>();
        (!faults.is_empty()).then(|| Arc::new(OwnedDnsQueryFaults(faults.into_boxed_slice())))
    }
}

#[cfg(test)]
#[path = "tests/faults.rs"]
mod tests;
