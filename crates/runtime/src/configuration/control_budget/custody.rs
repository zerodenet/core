//! Provenance is only one prerequisite of the actual retained command record.
use super::*;
/// Same live command permission's read-only provenance. This owns neither the
/// permission nor its request task and cannot extend its lifetime or refund it.
#[derive(Clone, Debug)]
pub struct ControlCommandCustody {
    pub(super) owner: Weak<ControlBudget>,
    pub(super) live: Weak<()>,
}
impl ControlCommandCustody {
    pub(crate) fn matches(&self, root: &Arc<ControlBudget>) -> bool {
        self.owner.ptr_eq(&Arc::downgrade(root)) && self.live.upgrade().is_some()
    }
}
impl ControlBudgetPermit {
    pub(crate) fn request_owner_matches(&self, root: &Arc<ControlBudget>) -> bool {
        matches!(self.charge, ControlCharge::RequestBytes(_)) && Arc::ptr_eq(&self.owner, root)
    }
    pub fn command_custody(&self) -> Option<ControlCommandCustody> {
        Some(ControlCommandCustody {
            owner: Arc::downgrade(&self.owner),
            live: Arc::downgrade(self.command_identity.as_ref()?),
        })
    }
}
