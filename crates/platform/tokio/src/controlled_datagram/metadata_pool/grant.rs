use super::*;
impl MetadataPoolGrants {
    pub fn retained(&self) -> Option<usize> {
        let owner = self.owner.upgrade()?;
        let count = owner.state.lock().ok()?.slots.len();
        Some(count)
    }
    pub fn reserve(&self, query: &TaskJoinWitness) -> Result<MetadataPoolPermit, ChannelError> {
        self.reserve_inner(Some(query), None, None)
            .map_err(|(error, returned)| {
                drop(returned);
                error
            })
    }
    pub(super) fn reserve_inner(
        &self,
        query: Option<&TaskJoinWitness>,
        preparing: Option<&TaskRegistrar>,
        mut protocol: Option<crate::contract::lifecycle::BoxChannelControl>,
    ) -> Result<
        MetadataPoolPermit,
        (
            ChannelError,
            Option<crate::contract::lifecycle::BoxChannelControl>,
        ),
    > {
        let reserved = (|| {
            let owner = self
                .owner
                .upgrade()
                .ok_or_else(|| error(Reason::Revoked, "metadata owner dropped"))?;
            if let Some(totals) = owner.totals.get() {
                totals.reconcile();
            }
            // A fixed library owner may have no Host Root. Scan only the ordinal
            // range present at entry, taking one actual Arc outside the owner lock.
            // Concurrent later reservations cannot extend this finite traversal.
            let end = owner.state.lock().expect("metadata pool poisoned").next;
            let mut previous = 0;
            loop {
                use std::ops::Bound::{Excluded, Included};
                let next = owner
                    .state
                    .lock()
                    .expect("metadata pool poisoned")
                    .slots
                    .range((Excluded(previous), Included(end)))
                    .next()
                    .map(|(ordinal, slot)| (*ordinal, slot.clone()));
                let Some((ordinal, slot)) = next else {
                    break;
                };
                previous = ordinal;
                MetadataPoolPermit(slot).release_registration_credit();
            }
            let registration_scope = preparing.map(TaskRegistrar::retirement_observation);
            let authority = owner
                .state
                .lock()
                .expect("metadata pool poisoned")
                .bound
                .clone()
                .ok_or_else(|| error(Reason::InvalidConfig, "unregistered metadata pool"))?;
            match (query, preparing) {
                (Some(query), None) => authority.check(&owner, query)?,
                (None, Some(tasks)) => {
                    if !matches!(
                        authority.declaration,
                        ResourceDeclaration::ControlledMetadataCarrier { .. }
                    ) || !authority.scope.matches(tasks)
                        || !authority.scope.resource_open()
                        || !authority.scope.registration_open()
                        || authority.host_cancel.is_cancelled()
                        || owner.cancel.is_cancelled()
                    {
                        return Err(error(
                            Reason::Revoked,
                            "metadata preparation requires its actual active owner scope",
                        ));
                    }
                }
                _ => {
                    return Err(error(
                        Reason::InvalidConfig,
                        "metadata original has no exact initializer",
                    ))
                }
            }
            let native = MetadataOwner::new(owner.limits).map_err(|source| {
                error(Reason::InvalidConfig, "invalid metadata limits").with_source(source)
            })?;
            let mut state = owner.state.lock().expect("metadata pool poisoned");
            if !state.live || state.stopped || owner.cancel.is_cancelled() {
                return Err(error(Reason::Revoked, "metadata admission closed"));
            }
            if state.slots.len() >= owner.limit.get() {
                return Err(error(
                    Reason::Capacity,
                    "metadata original capacity exhausted",
                ));
            }
            let ordinal = state
                .next
                .checked_add(1)
                .ok_or_else(|| error(Reason::Capacity, "metadata identity exhausted"))?;
            state.next = ordinal;
            let slot = Arc::new(Slot {
                owner: Arc::downgrade(&owner),
                instance: owner.instance,
                ordinal,
                closed: Arc::default(),
                retired: owner.cancel.child_token(),
                protocol_stop: OnceLock::new(),
                protocol: Mutex::new(None),
                waker: Mutex::default(),
                caller: {
                    let caller = OnceLock::new();
                    if let Some(query) = query {
                        let _ = caller.set(query.clone());
                    }
                    caller
                },
                preparing: std::sync::atomic::AtomicBool::new(preparing.is_some()),
                registration_uncertain: std::sync::atomic::AtomicBool::new(false),
                registration_scope,
                registration: MetadataPoolRegistration::new(&owner),
                close_cohort: OnceLock::new(),
                native: Mutex::new(Native {
                    owner: native,
                    control: None,
                    issuer: None,
                    transferred: false,
                    confirmed: None,
                    unbound: None,
                    acquired_confirmed: false,
                    attempted: false,
                    endpoints: None,
                    binding: None,
                    report: None,
                }),
            });
            if let Some(totals) = owner.totals.get() {
                totals.reserve_metadata(Arc::new(MetadataTotalsOriginal::new(
                    owner.clone(),
                    slot.clone(),
                )))?;
            }
            let cohort = sources::MetadataCloseCohort::new(owner.instance, owner.limits);
            slot.native
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .owner
                .install_close_sources(cohort.originals());
            assert!(slot.close_cohort.set(cohort).is_ok());
            // Both existing original books paid before phase heap is created.
            // No query/actor/native capability has escaped this owner gate.
            if let Some(control) = protocol.take() {
                let phase = crate::original_protocol::Phase::new(control, slot.instance);
                assert!(slot.protocol_stop.set(phase.cancellation()).is_ok());
                *slot
                    .protocol
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(phase);
            }
            state.slots.insert(ordinal, slot.clone());
            Ok(MetadataPoolPermit(slot))
        })();
        reserved.map_err(|error| (error, protocol))
    }
}
