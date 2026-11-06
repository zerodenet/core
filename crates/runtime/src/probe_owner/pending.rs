//! Original claims are bounded by their actual registered physical owners.
use super::*;
pub(super) struct Claim {
    pub(super) adoption: CarrierAdoption,
    owner: CarrierOwnerRegistration,
}
fn complete(adoption: &CarrierAdoption) -> bool {
    adoption
        .closed()
        .is_some_and(|native| native.matches(adoption))
        && adoption
            .users_closed()
            .is_some_and(|users| users.matches(adoption))
}
fn handed_off(adoption: &CarrierAdoption, originals: &[CarrierAdoption]) -> bool {
    adoption
        .retained_driver()
        .is_some_and(|proof| proof.retains(adoption))
        || originals.iter().any(|receiver| {
            !receiver.same_slot(adoption)
                && receiver
                    .retained_loan()
                    .is_some_and(|proof| proof.retains(adoption))
        })
}
impl ProbeCall {
    pub(super) fn retain(&self, adoption: CarrierAdoption) -> Result<(), ChannelError> {
        self.check_open()?;
        let owner = adoption
            .cleanup_owner()
            .ok_or_else(|| failure(Reason::Revoked))?;
        if !owner.matches_adoption(&adoption) {
            return Err(failure(Reason::StalePlan));
        }
        let originals: Vec<_> = lock(&self.0.pending)
            .iter()
            .map(|claim| claim.adoption.clone())
            .collect();
        let retired: Vec<_> = originals
            .iter()
            .filter(|slot| complete(slot) || handed_off(slot, &originals))
            .cloned()
            .collect();
        let mut pending = lock(&self.0.pending);
        if self.0.stop.is_cancelled() {
            return Err(failure(Reason::Revoked));
        }
        pending.retain(|claim| !retired.iter().any(|slot| slot.same_slot(&claim.adoption)));
        if pending
            .iter()
            .any(|claim| claim.adoption.same_slot(&adoption))
        {
            return Ok(());
        }
        if pending
            .iter()
            .filter(|claim| claim.owner.same_owner(&owner))
            .count()
            >= owner.limit().get()
        {
            return Err(failure(Reason::Capacity));
        }
        pending
            .try_reserve(1)
            .map_err(|source| failure(Reason::Capacity).with_source(source))?;
        pending.push(Claim { adoption, owner });
        drop(pending);
        if let Some(owner) = self.0.owner.upgrade() {
            owner.wake.signal();
        }
        Ok(())
    }
}
pub(super) async fn close(
    originals: Vec<CarrierAdoption>,
    deadline: Instant,
) -> Result<(), ChannelError> {
    for original in &originals {
        if complete(original) || handed_off(original, &originals) {
            continue;
        }
        let report = original.retire(deadline).await?;
        if !report.is_complete() || !complete(original) {
            return Err(failure(Reason::Timeout));
        }
    }
    Ok(())
}
