//! Conservative, nonblocking observations of the sole provider's full census.
use super::*;
#[derive(Clone, Debug)]
pub(in crate::component) struct BoundDatagramEvidence {
    pub ordinal: usize,
    pub first_fault: Option<SharedError>,
    confirmed: bool,
}
impl BoundDatagramEvidence {
    pub(in crate::component) fn physical_confirmed(&self) -> bool {
        self.confirmed
    }
}
impl HostBoundDatagramResources {
    pub(in crate::component) fn owns_binding(&self, binding: &DatagramBindingId) -> bool {
        self.issuer.owns(binding)
    }
    pub(in crate::component) fn evidence(&self, ordinal: usize) -> BoundDatagramEvidence {
        // No native poll, callback, close attempt or another owner's join read
        // occurs while holding this provider's gate. Busy means unavailable.
        let driver = self.driver.try_lock().ok().map(|driver| driver.clone());
        let driver_closed = match (&self.scope, driver) {
            (None, _) => true, // taskless peer sockets; original JoinedPeer is external
            (Some(_), Some(driver)) => driver.as_ref().is_none_or(TaskJoinWitness::is_joined),
            (Some(_), None) => false,
        };
        let Ok(state) = self.state.try_lock() else {
            return BoundDatagramEvidence {
                ordinal,
                first_fault: None,
                confirmed: false,
            };
        };
        // These durable fields are written from this same owner's take_report.
        // Stale cached live/Unknown records only keep evidence unavailable.
        BoundDatagramEvidence {
            ordinal,
            first_fault: state.fault.clone(),
            confirmed: state.phase == Phase::Revoked
                && driver_closed
                && !state.unknown
                && state.retained_live == 0
                && state.originals.is_empty()
                && state.owner.retained_original_count() == 0,
        }
    }
}
