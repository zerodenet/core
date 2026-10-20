//! Bounded same-slot observations; never whole-gateway native authorization.
use super::*;
use crate::runtime::inbound_owners::JoinedPeer;
use zero_platform_tokio::{
    contract::scope::TaskJoinWitness,
    controlled_carrier::{CarrierAdoption, CarrierClosed, CarrierUsersClosed},
};

/// Local evidence only. The containing raw-IP record can still have other
/// unaccounted native gateways (notably DNS); this never removes that record.
pub(crate) struct NativeCarrierCompletion {
    owner: Weak<Mutex<State>>,
    id: u64,
    native: CarrierClosed,
    users: CarrierUsersClosed,
    caller: Arc<JoinedPeer>,
    witness: TaskJoinWitness,
}
impl NativeCarrierCompletion {
    pub(crate) fn matches(&self, registration: &NativeRegistration) -> bool {
        self.id == registration.id
            && self.owner.ptr_eq(&registration.owner)
            && self.native.had_native_acquisition()
            && self.users.had_native_acquisition()
            && self.users.matches_caller(&self.witness)
            && self.users.caller_exit().is_some_and(|exit| {
                exit.id() == self.caller.task() && exit.class() == self.caller.class()
            })
    }
}
impl NativeRegistration {
    /// Called only after the actual private issuer bound the original caller
    /// to this grant, before native prepare. This carries no new authority.
    /// One exact slot is idempotent. A distinct slot permanently marks this
    /// record unknown and is rejected before its native preparation can begin.
    pub(crate) fn observe_carrier(&self, adoption: CarrierAdoption) -> Result<(), ChannelError> {
        self.check_open()?;
        let owner = self
            .owner
            .upgrade()
            .ok_or_else(|| rejected(Reason::Revoked))?;
        let mut state = owner.lock().unwrap_or_else(|error| error.into_inner());
        let record = state
            .records
            .get_mut(&self.id)
            .ok_or_else(|| rejected(Reason::Revoked))?;
        if record.kind != ChildKind::Tcp || !record.entered || record.carrier_conflict {
            return Err(rejected(Reason::InvalidConfig));
        }
        if let Some(actual) = &record.carrier {
            if actual.same_slot(&adoption) {
                return Ok(());
            }
            record.carrier_conflict = true;
            return Err(
                rejected(Reason::Unsupported).with_source(std::io::Error::other(
                    "one raw TCP caller entered a second controlled carrier reservation",
                )),
            );
        }
        record.carrier = Some(adoption);
        Ok(())
    }
    /// Read only the same actual native, users and caller receipts. No unused
    /// reservation, expired weak owner, empty count or task-only join qualifies.
    pub(crate) fn carrier_completion(&self) -> Option<NativeCarrierCompletion> {
        let owner = self.owner.upgrade()?;
        let (adoption, caller) = {
            let state = owner.lock().unwrap_or_else(|error| error.into_inner());
            let record = state.records.get(&self.id)?;
            if !record.entered || record.carrier_conflict {
                return None;
            }
            (record.carrier.clone()?, record.caller.clone()?)
        };
        let witness = caller.join_witness()?;
        let joined = caller.observed_join()?;
        let actual = witness.try_joined()?;
        if actual.id() != joined.task() || actual.class() != joined.class() {
            return None;
        }
        let native = adoption.closed()?;
        let users = adoption.users_closed()?;
        if !native.matches(&adoption)
            || !users.matches(&adoption)
            || !native.had_native_acquisition()
            || !users.had_native_acquisition()
            || !users.matches_caller(&witness)
        {
            return None;
        }
        let receipt = NativeCarrierCompletion {
            owner: self.owner.clone(),
            id: self.id,
            native,
            users,
            caller: joined,
            witness,
        };
        receipt.matches(self).then_some(receipt)
    }
}
