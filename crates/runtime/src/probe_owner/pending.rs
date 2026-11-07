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
        || adoption
            .retained_preparation_job()
            .is_some_and(|proof| proof.retains(adoption))
        || originals.iter().any(|receiver| {
            !receiver.same_slot(adoption)
                && receiver
                    .retained_loan()
                    .is_some_and(|proof| proof.retains(adoption))
        })
}
pub(super) fn settled(originals: &[CarrierAdoption]) -> bool {
    originals
        .iter()
        .all(|original| complete(original) || handed_off(original, originals))
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
    let mut faults = Vec::new();
    for original in &originals {
        if complete(original) || handed_off(original, &originals) {
            continue;
        }
        match original.retire(deadline).await {
            Ok(report) if report.is_complete() && complete(original) => {}
            Ok(report) => faults.push(Arc::new(
                failure(Reason::Timeout).with_source(NativeUnfinished(report)),
            )),
            Err(source) => faults.push(Arc::new(source)),
        }
    }
    combine(faults)
}

/// This phase never waits for the executing caller's own join or Host ACK.
/// All claims progress even if an earlier native control fails/retains unknown.
pub(super) async fn close_native(
    originals: Vec<CarrierAdoption>,
    end: Instant,
) -> Result<(), ChannelError> {
    let mut faults = Vec::new();
    for original in &originals {
        if handed_off(original, &originals) {
            continue;
        }
        match original.retire_native(end).await {
            Ok(closed)
                if closed.report().is_complete()
                    && closed
                        .closed()
                        .is_some_and(|receipt| receipt.matches(original)) => {}
            Ok(closed) => faults.push(Arc::new(
                failure(Reason::Timeout).with_source(NativeUnfinished(closed.into_report())),
            )),
            Err(source) => faults.push(Arc::new(source)),
        }
    }
    combine(faults)
}
pub(super) fn combine(faults: Vec<Arc<ChannelError>>) -> Result<(), ChannelError> {
    if faults.is_empty() {
        Ok(())
    } else {
        Err(retained(&faults[0]).with_source(NativeFaults(faults)))
    }
}
#[derive(Debug)]
struct NativeUnfinished(zero_platform_tokio::contract::lifecycle::CloseReport);
impl std::fmt::Display for NativeUnfinished {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "actual native probe cleanup unfinished: {:?}", self.0)
    }
}
impl std::error::Error for NativeUnfinished {}
#[derive(Debug)]
struct NativeFaults(Vec<Arc<ChannelError>>);
impl std::fmt::Display for NativeFaults {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for error in &self.0 {
            write!(f, "; {error}")?;
        }
        Ok(())
    }
}
impl std::error::Error for NativeFaults {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.0.first().map(|e| e.as_ref() as _)
    }
}
