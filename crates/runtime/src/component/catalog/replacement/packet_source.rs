//! Retain the existing replacement before revoking its actual cached borrowers.
use super::*;
use zero_platform_tokio::controlled_carrier::CarrierAdoption;

/// Serializes this same catalog's Source StopOld phase. Drop only releases the
/// transition lock: the persisted replacement keeps originals and admission
/// fences until ordinary confirmed retirement/publication. It proves no close.
#[must_use]
pub struct PacketSourceRetirement<'a> {
    _transition: tokio::sync::MutexGuard<'a, ()>,
}
impl ComponentCatalog {
    /// The owning Source coordinator must first fence/check all of its actual
    /// business users. The supplied pause identifies only proven cached loans
    /// in this exact candidate's StopOld set. This method cancels those old
    /// component Scopes; it neither closes a Source nor starts any replacement.
    pub fn begin_packet_source_retirement<'a>(
        &'a self,
        candidate: &Arc<ComponentPlan>,
        pause: Arc<ReplacementUsersPause>,
    ) -> Result<Option<PacketSourceRetirement<'a>>, ChannelError> {
        if !pause
            .candidate
            .upgrade()
            .is_some_and(|actual| Arc::ptr_eq(&actual, candidate))
        {
            return Err(invalid(
                Reason::StalePlan,
                "Source retirement pause belongs to another candidate",
            ));
        }
        let mut receivers: Vec<CarrierAdoption> = Vec::new();
        for root in &pause._roots {
            for receiver in root.receivers() {
                if !receivers.iter().any(|actual| actual.same_slot(receiver)) {
                    receivers.push(receiver.clone());
                }
            }
        }
        if receivers.is_empty() {
            return Ok(None);
        }
        let transition = self.transition.try_lock().map_err(|_| {
            invalid(
                Reason::Capacity,
                "another actual component transition remains owned",
            )
        })?;
        let loan_fence = self.admission.freeze_loans();
        let pending = {
            let state = self.state.lock().expect("component catalog poisoned");
            if state.maintenance
                || !state
                    .current
                    .iter()
                    .chain(state.staged.iter())
                    .chain(state.cold.iter())
                    .filter_map(|entry| entry.plan.as_ref().ok())
                    .any(|actual| Arc::ptr_eq(actual, candidate))
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "Source retirement candidate is not owned by this catalog",
                ));
            }
            let pending = if let Some(pending) = state.replacement.as_ref() {
                if !Arc::ptr_eq(&pending.candidate, candidate) || pending.released() {
                    return Err(invalid(
                        Reason::Capacity,
                        "another replacement still owns its original retirement",
                    ));
                }
                pending.clone()
            } else {
                Arc::new(Replacement::new(
                    candidate.clone(),
                    state.current.as_ref().map(Current::ready),
                    state
                        .staged
                        .iter()
                        .map(|entry| entry.ready().clone())
                        .chain(state.retired.iter().cloned()),
                )?)
            };
            pending
        };
        // Provider/Scope inspection can run observers: keep it outside catalog
        // state. The serial transition guard prevents another publication.
        let slots = pending.cached_receiver_slots(&receivers)?;
        {
            let mut state = self.state.lock().expect("component catalog poisoned");
            let current = state
                .current
                .as_ref()
                .and_then(|entry| entry.plan.as_ref().ok());
            if state.maintenance || !pending.matches_current(current) {
                return Err(invalid(
                    Reason::StalePlan,
                    "Source retirement ownership changed",
                ));
            }
            // Persist every original before cancellation or a Source await.
            state.replacement = Some(pending.clone());
        }
        if !pending.retains_budget_pause() {
            pending.retain_budget_pause(pause);
        }
        loan_fence.retain();
        // Only the serial transition guard remains held, as in normal start.
        // Actual owner state/census/catalog locks never surround callbacks.
        for slot in slots {
            slot.revoke();
        }
        self.admission.signal().notify_waiters();
        Ok(Some(PacketSourceRetirement {
            _transition: transition,
        }))
    }
}
impl Replacement {
    fn cached_receiver_slots(
        &self,
        receivers: &[CarrierAdoption],
    ) -> Result<Vec<Arc<ComponentSlot>>, ChannelError> {
        let mut result = Vec::new();
        for receiver in receivers {
            let slot = self
                .parts
                .iter()
                .find_map(|part| {
                    if !part.instances.contains(&receiver.instance()) {
                        return None;
                    }
                    part.plan
                        .order
                        .iter()
                        .find(|slot| slot.instance_id() == receiver.instance())
                        .cloned()
                })
                .ok_or_else(|| {
                    invalid(
                        Reason::StalePlan,
                        "cached receiver is outside the actual retiring subset",
                    )
                })?;
            if !slot
                .recipe
                .spec()
                .resources
                .iter()
                .enumerate()
                .any(|(ordinal, _)| {
                    slot.original_carrier_owner(ordinal)
                        .is_some_and(|owner| owner.matches_adoption(receiver))
                })
            {
                return Err(invalid(
                    Reason::StalePlan,
                    "cached receiver no longer names its actual original provider",
                ));
            }
            if !result.iter().any(|actual| Arc::ptr_eq(actual, &slot)) {
                result.push(slot);
            }
        }
        Ok(result)
    }
}
