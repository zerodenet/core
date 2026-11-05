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
    /// Only actual Node-role entry points can admit a foreign parent. The
    /// parent's task-local original record/witness and the same host budget
    /// are checked before a child is registered in this distinct actual Scope.
    pub(crate) async fn bootstrap<T>(&self, work: impl Future<Output = T>) -> io::Result<T> {
        if let Some((record, caller)) = QUERY_RECORD
            .try_with(Clone::clone)
            .ok()
            .zip(QUERY_ORIGIN.try_with(Clone::clone).ok())
        {
            if !record.budget.same_budget(&self.0.budget)
                || caller.is_joined()
                || !record
                    .registration
                    .get()
                    .and_then(Option::as_ref)
                    .is_some_and(|actual| actual.witness.same_registration(&caller))
            {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "bootstrap parent is not an active original of this host budget",
                ));
            }
        }
        Ok(BOOTSTRAP_TARGET.scope(Arc::downgrade(&self.0), work).await)
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
            let authorized = BOOTSTRAP_TARGET
                .try_with(std::sync::Weak::upgrade)
                .ok()
                .flatten()
                .is_some_and(|target| Arc::ptr_eq(&target, &self.0))
                && record.budget.same_budget(&self.0.budget)
                && record
                    .registration
                    .get()
                    .and_then(Option::as_ref)
                    .is_some_and(|actual| actual.witness.same_registration(&caller));
            if !authorized {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "nested DNS work crossed its original Scope",
                ));
            }
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
        self.close_descendants_inner(caller, end).await
    }
    pub(crate) async fn close_bootstrap_descendants(
        &self,
        caller: &TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> io::Result<()> {
        // This only waits for already sealed ancestry in this original ledger.
        // It cannot grant cross-Scope acquisition, invent a parent or cancel a
        // shared child still used by another real waiter.
        self.close_descendants_inner(caller, end).await
    }
    async fn close_descendants_inner(
        &self,
        caller: &TaskJoinWitness,
        end: tokio::time::Instant,
    ) -> io::Result<()> {
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
