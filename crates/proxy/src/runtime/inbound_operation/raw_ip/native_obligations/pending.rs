//! Pre-delivery cleanup belongs to the same raw caller, never to its first user.
use super::*;
use zero_platform_tokio::controlled_carrier::{CarrierAdoption, CarrierOwnerRegistration};
fn pending_error(reason: Reason) -> ChannelError {
    let mut error = rejected(reason);
    error.stage = Stage::Prepare;
    error.delivery = zero_platform_tokio::contract::DeliveryState::NotSubmitted;
    error
}
pub(super) struct Claim {
    pub(super) adoption: CarrierAdoption,
    owner: CarrierOwnerRegistration,
}
fn complete(slot: &CarrierAdoption) -> bool {
    slot.closed().is_some_and(|native| native.matches(slot))
        && slot.users_closed().is_some_and(|users| users.matches(slot))
}
// The receiver must really hold the exact controller. Read-only origin ancestry
// and related_control never substitute for this sealed strong-custody proof.
fn handed_off(slot: &CarrierAdoption, originals: &[CarrierAdoption]) -> bool {
    slot.retained_driver()
        .is_some_and(|proof| proof.retains(slot))
        || originals.iter().any(|receiver| {
            !receiver.same_slot(slot)
                && receiver
                    .retained_loan()
                    .is_some_and(|proof| proof.retains(slot))
        })
}
impl NativeRegistration {
    /// Cleanup only. The observer independently validates current plan, Engine
    /// admission and every selected actual provider before reaching this method.
    pub(crate) fn observe_pending_carrier(
        &self,
        adoption: CarrierAdoption,
    ) -> Result<(), ChannelError> {
        self.check_open()?;
        let physical = adoption
            .cleanup_owner()
            .ok_or_else(|| pending_error(Reason::Revoked))?;
        if !physical.matches_adoption(&adoption) {
            return Err(pending_error(Reason::StalePlan));
        }
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| pending_error(Reason::Revoked))?;
        let originals = {
            let state = owner.lock().unwrap_or_else(|error| error.into_inner());
            let record = state
                .records
                .get(&self.id)
                .ok_or_else(|| pending_error(Reason::Revoked))?;
            if record.kind != ChildKind::Tcp || !record.entered || record.retiring {
                return Err(pending_error(Reason::Revoked));
            }
            record
                .pending_carriers
                .iter()
                .map(|claim| claim.adoption.clone())
                .chain(
                    record
                        .carrier
                        .iter()
                        .chain(&record.additional_carriers)
                        .cloned(),
                )
                .collect::<Vec<_>>()
        };
        let removed = originals
            .iter()
            .filter(|slot| complete(slot) || handed_off(slot, &originals))
            .cloned()
            .collect::<Vec<_>>();
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        if state.closing {
            return Err(pending_error(Reason::Revoked));
        }
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| pending_error(Reason::Revoked))?;
        if record.retiring
            || record
                .caller
                .as_ref()
                .is_none_or(|caller| caller.cancellation().is_cancelled())
        {
            return Err(pending_error(Reason::Revoked));
        }
        record
            .pending_carriers
            .retain(|claim| !removed.iter().any(|slot| slot.same_slot(&claim.adoption)));
        if record
            .carrier
            .iter()
            .chain(&record.additional_carriers)
            .any(|slot| slot.same_slot(&adoption))
            || record
                .pending_carriers
                .iter()
                .any(|claim| claim.adoption.same_slot(&adoption))
        {
            return Ok(());
        }
        if record
            .pending_carriers
            .iter()
            .filter(|claim| claim.owner.same_owner(&physical))
            .count()
            >= physical.limit().get()
        {
            return Err(pending_error(Reason::Capacity));
        }
        record
            .pending_carriers
            .try_reserve(1)
            .map_err(|source| pending_error(Reason::Capacity).with_source(source))?;
        record.pending_carriers.push(Claim {
            adoption,
            owner: physical,
        });
        Ok(())
    }
    pub(super) async fn settle_pending(&self, deadline: Instant) -> Result<bool, ChannelError> {
        let Some(owner) = self.owner.upgrade() else {
            return Ok(false);
        };
        let (caller, originals, receivers) = {
            let state = owner.lock().unwrap_or_else(|error| error.into_inner());
            let Some(record) = state.records.get(&self.id) else {
                return Ok(true);
            };
            if record.pending_carriers.is_empty() {
                return Ok(true);
            }
            if !record.retiring {
                return Ok(false);
            }
            let originals = record
                .pending_carriers
                .iter()
                .map(|claim| claim.adoption.clone())
                .collect::<Vec<_>>();
            let receivers = originals
                .iter()
                .cloned()
                .chain(
                    record
                        .carrier
                        .iter()
                        .chain(&record.additional_carriers)
                        .cloned(),
                )
                .collect::<Vec<_>>();
            (record.caller.clone(), originals, receivers)
        };
        let Some(caller) = caller else {
            return Ok(false);
        };
        let Some(joined) = caller.observed_join() else {
            return Ok(false);
        };
        let Some(witness) = caller.join_witness() else {
            return Ok(false);
        };
        if witness
            .try_joined()
            .is_none_or(|actual| actual.id() != joined.task() || actual.class() != joined.class())
        {
            return Ok(false);
        }
        let mut removed = Vec::new();
        for slot in &originals {
            if complete(slot) || handed_off(slot, &receivers) {
                removed.push(slot.clone());
                continue;
            }
            let report = slot.retire(deadline).await?;
            if report.is_complete() && complete(slot) {
                removed.push(slot.clone());
            }
        }
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let Some(record) = state.records.get_mut(&self.id) else {
            return Ok(true);
        };
        record
            .pending_carriers
            .retain(|claim| !removed.iter().any(|slot| slot.same_slot(&claim.adoption)));
        Ok(record.pending_carriers.is_empty())
    }
}
