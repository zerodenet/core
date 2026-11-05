//! Original nested DNS work stays in the same finite task ledger.
use super::*;
impl OwnedDnsQueryTasks {
    pub(crate) fn bind_finalizer(
        &self,
        provider: std::sync::Weak<dyn crate::native::NativeDnsProvider>,
    ) -> io::Result<()> {
        if let Err(provider) = self.0.finalizer.set(provider) {
            if !self
                .0
                .finalizer
                .get()
                .is_some_and(|current| std::sync::Weak::ptr_eq(current, &provider))
            {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "DNS task original provider already bound",
                ));
            }
        }
        Ok(())
    }
    pub(super) fn inherited_tasks(&self) -> io::Result<Vec<TaskJoinWitness>> {
        let Some((record, caller)) = QUERY_RECORD
            .try_with(Clone::clone)
            .ok()
            .zip(QUERY_ORIGIN.try_with(Clone::clone).ok())
        else {
            return Ok(Vec::new());
        };
        if !caller
            .scope_identity()
            .same_scope(&self.0.registrar.scope_identity())
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "nested DNS work crossed its original Scope",
            ));
        }
        self.ancestors_of(&record, &caller)
    }
    pub(super) fn ancestors_of(
        &self,
        record: &Record,
        caller: &TaskJoinWitness,
    ) -> io::Result<Vec<TaskJoinWitness>> {
        if record.ancestors.len() >= self.0.limit.get().saturating_mul(3) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "nested DNS ancestry exhausted the finite Scope budget",
            ));
        }
        let mut ancestors = record.ancestors.clone();
        ancestors.try_reserve(1).map_err(io::Error::other)?;
        ancestors.push(caller.clone());
        Ok(ancestors)
    }
    /// Wait for actual nested jobs, including non-cancellable blocking lookup
    /// originals. This never cancels a shared lookup still used by another
    /// caller and never joins a persistent driver owned independently by a pool.
    pub async fn close_descendants(
        &self,
        caller: &TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> io::Result<()> {
        if !caller
            .scope_identity()
            .same_scope(&self.0.registrar.scope_identity())
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "DNS descendant cleanup requires the original Scope",
            ));
        }
        loop {
            self.reap();
            let originals = self
                .0
                .state
                .lock()
                .expect("DNS query tasks poisoned")
                .records
                .iter()
                .filter(|record| {
                    record
                        .ancestors
                        .iter()
                        .any(|parent| parent.same_registration(caller))
                })
                .cloned()
                .collect::<Vec<_>>();
            if originals.is_empty() {
                return self
                    .0
                    .state
                    .lock()
                    .expect("DNS query tasks poisoned")
                    .first_failure
                    .as_ref()
                    .map_or(Ok(()), |source| Err(io::Error::other(source.clone())));
            }
            for original in originals {
                let registration = async {
                    loop {
                        let bound = original.bound.notified();
                        tokio::pin!(bound);
                        bound.as_mut().enable();
                        if let Some(registration) = original.registration.get() {
                            return registration.as_ref();
                        }
                        bound.await;
                    }
                };
                if let Some(registration) = tokio::time::timeout_at(end, registration)
                    .await
                    .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
                {
                    tokio::time::timeout_at(end, registration.witness.wait())
                        .await
                        .map_err(|source| io::Error::new(io::ErrorKind::TimedOut, source))?
                        .result()
                        .map_err(io::Error::other)?;
                }
            }
            // A child can register its own blocking work just before its join.
            // Every descendant retains the sealed ancestry after parents reap.
        }
    }
}
