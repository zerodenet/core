use super::*;
impl HostResources {
    /// Actual host factory only. No ServicePurpose, DNS builder or protocol can
    /// obtain this issuer from ResourceAccess. One private root per instance.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "explicit root-DNS library path; production activation requires actual host task and resource grants"
        )
    )]
    pub(in crate::component) fn issue_datagram_root(
        self: &Arc<Self>,
        ordinals: &[usize],
        egress: EgressInterfaceControl,
        tasks: &TaskRegistrar,
        max_queries: NonZeroUsize,
    ) -> Result<Vec<RootDatagramGrant>, ChannelError> {
        let (instance, scope) = self.roots.identity.as_ref().ok_or_else(|| self.denied())?;
        if !scope.matches(tasks) {
            return Err(self.denied());
        }
        let selected: BTreeSet<_> = ordinals.iter().copied().collect();
        if selected.len() != ordinals.len()
            || selected.is_empty()
            || selected.iter().any(|ordinal| {
                self.roots
                    .entries
                    .get(ordinal)
                    .is_none_or(|entry| entry.generation.0 != egress.generation())
            })
        {
            return Err(self.denied());
        }
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.closed
            || self.cancel.is_cancelled()
            || state.start_deadline.is_some()
            || self.roots.live.load(Ordering::Acquire)
        {
            return Err(self.denied());
        }
        let mut root = self
            .roots
            .context
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if root.is_some() {
            return Err(self.denied());
        }
        let authority = RootDatagramAuthority::issue();
        let access: Arc<dyn ResourceAccess> = self.clone();
        let grants = ordinals
            .iter()
            .map(|ordinal| {
                let entry = &self.roots.entries[ordinal];
                authority.grant(
                    Arc::downgrade(&access),
                    *instance,
                    *ordinal,
                    entry.peer,
                    entry.generation,
                )
            })
            .collect();
        *root = Some(Arc::new(RootContext {
            authority,
            egress,
            ordinals: selected,
            max_queries,
            queries: Mutex::new(Vec::new()),
        }));
        Ok(grants)
    }
}
impl RootContext {
    pub(super) fn register_query(&self, witness: &TaskJoinWitness) -> Result<(), ()> {
        let snapshot = self
            .queries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let retired: Vec<_> = snapshot
            .iter()
            .filter(|query| query.is_joined())
            .cloned()
            .collect();
        let mut queries = self
            .queries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut dropped = Vec::new();
        let mut index = 0;
        while index < queries.len() {
            if retired
                .iter()
                .any(|old| old.same_registration(&queries[index]))
            {
                dropped.push(queries.remove(index));
            } else {
                index += 1;
            }
        }
        let result = if queries.iter().any(|query| query.same_registration(witness)) {
            Ok(())
        } else if queries.len() >= self.max_queries.get() {
            Err(())
        } else {
            queries.push(witness.clone());
            Ok(())
        };
        drop(queries);
        drop(dropped);
        result
    }
}
