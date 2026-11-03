//! Actual already-authorized carrier originals belong to the same accepted graph.
//! This observes the physical owner's existing finite slots; it grants no I/O.
use super::*;
use crate::controlled_carrier::{CarrierAdoption, CarrierOwnerRegistration};

#[derive(Clone)]
pub(super) struct CarrierUse {
    owner: CarrierOwnerRegistration,
    slot: CarrierAdoption,
    caller: TaskJoinWitness,
    fault: bool,
}
impl AcceptedCaller {
    /// Called by the real business issuer after binding the original caller and
    /// before native preparation. No synthetic IDs, pool or second task owner.
    pub fn observe_carrier_with_owner(
        &self,
        physical: &CarrierOwnerRegistration,
        slot: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check_open()?;
        let users = self.users.upgrade().ok_or_else(gone)?;
        let caller = self.task_join_witness().ok_or_else(gone)?;
        if !physical.matches_adoption(&slot) || !slot.matches_bound_caller(&caller) {
            return Err(failure(
                Stage::Prepare,
                Reason::StalePlan,
                "carrier differs from actual accepted caller",
            ));
        }
        // Only original closed, unacquired reservations may be pruned before
        // caller join. Get original slot observations outside the graph lock.
        let observed: Vec<_> = users
            .state()
            .carriers
            .iter()
            .map(|record| record.slot.clone())
            .collect();
        let unused: Vec<_> = observed
            .into_iter()
            .filter(|slot| {
                slot.closed()
                    .is_some_and(|closed| closed.matches(slot) && !closed.had_native_acquisition())
            })
            .collect();
        let mut removed = Vec::new();
        removed.try_reserve(unused.len()).map_err(|source| {
            failure(
                Stage::Prepare,
                Reason::Capacity,
                "accepted carrier release exhausted",
            )
            .with_source(source)
        })?;
        let mut state = users.state();
        if state.sealed || users.cancel.is_cancelled() {
            return Err(gone());
        }
        for index in (0..state.carriers.len()).rev() {
            if unused
                .iter()
                .any(|unused| unused.same_slot(&state.carriers[index].slot))
            {
                removed.push(state.carriers.swap_remove(index));
            }
        }
        if let Some(actual) = state
            .carriers
            .iter()
            .find(|record| record.slot.same_slot(&slot))
        {
            return if actual.caller.same_registration(&caller) {
                Ok(())
            } else {
                Err(gone())
            };
        }
        if state
            .carriers
            .iter()
            .filter(|record| record.owner.same_owner(physical))
            .count()
            >= physical.limit().get()
        {
            return Err(failure(
                Stage::Prepare,
                Reason::Capacity,
                "actual physical carrier capacity exhausted",
            ));
        }
        state.carriers.try_reserve(1).map_err(|source| {
            failure(
                Stage::Prepare,
                Reason::Capacity,
                "accepted carrier custody exhausted",
            )
            .with_source(source)
        })?;
        state.carriers.push(CarrierUse {
            owner: physical.clone(),
            slot,
            caller,
            fault: false,
        });
        Ok(())
    }
}
impl Users {
    pub(super) fn revoke_carriers(&self) {
        let slots: Vec<_> = self
            .state()
            .carriers
            .iter()
            .map(|record| record.slot.clone())
            .collect();
        for slot in slots {
            slot.request_retirement();
        }
    }
    pub(super) fn has_carrier_caller(&self, caller: &TaskJoinWitness) -> bool {
        self.state()
            .carriers
            .iter()
            .any(|record| record.caller.same_registration(caller))
    }
    pub(super) async fn retire_joined_carriers(&self, deadline: Instant) -> bool {
        self.close_carrier_records(deadline, true).await
    }
    pub(super) async fn close_carriers(&self, deadline: Instant) -> bool {
        self.close_carrier_records(deadline, false).await
    }
    async fn close_carrier_records(&self, deadline: Instant, joined_only: bool) -> bool {
        // Cancelled cleanup leaves the same originals installed. The actual
        // observer and final shutdown serialize consumption of their receipts.
        let Ok(_cleanup) = tokio::time::timeout_at(deadline, self.carrier_cleanup.lock()).await
        else {
            return false;
        };
        loop {
            let Some(record) = self
                .state()
                .carriers
                .iter()
                .find(|record| !joined_only || record.caller.is_joined())
                .cloned()
            else {
                return true;
            };
            if record.fault {
                return false;
            }
            let mut report = match record.slot.retire(deadline).await {
                Ok(report) => report,
                Err(source) => {
                    self.remember(Arc::new(source));
                    return false;
                }
            };
            let receipts = record.slot.closed().is_some_and(|closed| {
                closed.matches(&record.slot)
                    && (!closed.had_native_acquisition()
                        || record.slot.users_closed().is_some_and(|users| {
                            users.matches(&record.slot)
                                && users.matches_caller(&record.caller)
                                && users.caller_exit().is_some()
                        }))
            });
            let failed = !report.failed_tasks.is_empty();
            let complete = report.is_complete() && receipts && !failed;
            if failed {
                let actual = report.failed_tasks.remove(0);
                let source = Arc::new(actual.error);
                report.failed_tasks.insert(
                    0,
                    crate::contract::lifecycle::TaskFailure {
                        id: actual.id,
                        class: actual.class,
                        error: retained(source.clone()),
                    },
                );
                self.remember(source);
                if let Some(actual) = self
                    .state()
                    .carriers
                    .iter_mut()
                    .find(|actual| actual.slot.same_slot(&record.slot))
                {
                    actual.fault = true;
                }
            }
            // Persist exact positive receipts before another await. Successful
            // totals use one ordinal; actual failed task IDs/sources are retained.
            report.unfinished.clear();
            if !self.append_report(usize::MAX, report) {
                return false;
            }
            if !complete {
                return false;
            }
            let original = {
                let mut state = self.state();
                let index = state
                    .carriers
                    .iter()
                    .position(|actual| actual.slot.same_slot(&record.slot));
                index.map(|index| state.carriers.remove(index))
            };
            drop(original);
        }
    }
}
fn gone() -> ChannelError {
    failure(
        Stage::Prepare,
        Reason::Revoked,
        "accepted carrier owner retired",
    )
}
