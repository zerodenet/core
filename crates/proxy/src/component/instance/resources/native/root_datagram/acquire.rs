use super::*;
use zero_traits::DialPolicy;
impl HostResources {
    pub(in crate::component::instance::resources::native) fn open_root(
        &self,
        request: &RootDatagramRequest,
        query: &TaskJoinWitness,
    ) -> Result<GrantedRootDatagram, RootDatagramAttemptFailure> {
        let deny = || RootDatagramAttemptFailure::denied(self.denied());
        let (instance, scope) = self.roots.identity.as_ref().ok_or_else(deny)?;
        let context = self.roots.context().ok_or_else(deny)?;
        let entry = self
            .roots
            .entries
            .get(&request.ordinal())
            .ok_or_else(deny)?;
        // Scope getters and actual selection run outside all provider locks.
        if request.instance() != *instance
            || !self.roots.admitted(&self.cancel)
            || !scope.resource_open()
            || !scope.same_scope(&query.scope_identity())
            || query.is_joined()
            || !context.authority.matches_request(request)
            || !context.ordinals.contains(&request.ordinal())
            || request.peer() != entry.peer
            || request.network_generation() != entry.generation
        {
            return Err(deny());
        }
        let policy = DialPolicy::default();
        let selection = context
            .egress
            .select_for_peer_with_policy(entry.peer, &policy)
            .map_err(|source| {
                RootDatagramAttemptFailure::denied(
                    error(
                        self.component,
                        Reason::Io,
                        "root UDP actual egress selection failed",
                    )
                    .with_source(source),
                )
            })?;
        if selection.generation() != entry.generation.0 {
            return Err(deny());
        }
        entry.reap_queries(self.component);
        // An unresolved original or an unfinished acquisition cannot be
        // repaired by another query. Do not create a second charged record
        // referring to an already-Unknown original reservation.
        if entry
            .ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .native_unconfirmed
            || entry.queries().iter().any(|old| {
                old.attempt
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .opening
            })
        {
            return Err(deny());
        }
        context.register_query(query).map_err(|_| {
            RootDatagramAttemptFailure::denied(error(
                self.component,
                Reason::Capacity,
                "root query record ceiling exhausted",
            ))
        })?;
        let record = Arc::new(Query {
            witness: query.clone(),
            attempt: Mutex::new(Attempt {
                opening: true,
                settled_without_view: false,
                control: None,
                usage: None,
            }),
        });
        {
            let mut records = entry
                .records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !self.roots.admitted(&self.cancel)
                || records.len() >= context.max_queries.get()
                || records
                    .iter()
                    .any(|old| old.witness.same_registration(query))
            {
                return Err(deny());
            }
            // One native attempt per original query and exact candidate. This
            // reproduces exchange_with's original one bind, not packet retries.
            records.push(record.clone());
        }
        let (socket, control) = {
            let mut owner = entry
                .native
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            entry.attempted.store(true, Ordering::Release);
            match owner.acquire(entry.peer, None, &policy, &selection) {
                Ok(socket) => {
                    let control =
                        Arc::new(owner.take_control(&socket).expect("new original control"));
                    // Persist same control before view publication, observers,
                    // generation postchecks or any await. No external callback.
                    record
                        .attempt
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .control = Some(control.clone());
                    (socket, control)
                }
                Err(acquisition) => {
                    RootDeclarations::fault(
                        entry,
                        error(
                            self.component,
                            Reason::Io,
                            "root UDP native acquisition failed",
                        )
                        .with_source(acquisition.source.clone()),
                    );
                    let failure = context.authority.acquisition_failure(
                        request,
                        query,
                        error(
                            self.component,
                            Reason::Io,
                            "root UDP native acquisition failed",
                        ),
                        acquisition,
                        &owner,
                    );
                    let mut state = record
                        .attempt
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state.opening = false;
                    state.settled_without_view = failure.retry_receipt().is_some();
                    return Err(failure);
                }
            }
        };
        let (io, usage) = context
            .authority
            .install_io(request, query, socket, &control)
            .map_err(RootDatagramAttemptFailure::denied)?;
        {
            let mut state = record
                .attempt
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.usage = Some(usage);
            state.opening = false;
        }
        // A stale result never rolls back/drops native ownership. The view
        // returns; the persistent exact control remains charged for cleanup.
        if !self.roots.admitted(&self.cancel)
            || context.egress.generation() != entry.generation.0
            || query.is_joined()
        {
            drop(io);
            control.revoke();
            return Err(deny());
        }
        Ok(GrantedRootDatagram {
            io,
            selection,
            instance: *instance,
        })
    }
}
