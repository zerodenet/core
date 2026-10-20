//! Same actual owner, immutable acquisition outcome and exact original source.
use super::*;
use std::sync::Weak;

#[derive(Debug)]
pub(super) struct AcquisitionEvidence {
    owner: Weak<()>,
    reservation: Option<DatagramReservationId>,
    outcome: AcquisitionOutcome,
    source: Arc<std_io::Error>,
}
impl ControlledDatagramOwner {
    pub(super) fn acquisition_error(
        &self,
        reservation: Option<DatagramReservationId>,
        outcome: AcquisitionOutcome,
        source: Arc<std_io::Error>,
    ) -> DatagramAcquisitionError {
        DatagramAcquisitionError {
            reservation,
            outcome,
            evidence: AcquisitionEvidence {
                owner: Arc::downgrade(&self.identity),
                reservation,
                outcome,
                source: source.clone(),
            },
            source,
        }
    }
    /// Does not close anything or upgrade an Unknown. Public diagnostic fields
    /// and equal numeric reservation IDs from another owner cannot forge this.
    pub fn confirms_settled_acquisition(&self, failure: &DatagramAcquisitionError) -> bool {
        let evidence = &failure.evidence;
        if !evidence.owner.ptr_eq(&Arc::downgrade(&self.identity))
            || evidence.reservation != failure.reservation
            || evidence.outcome != failure.outcome
            || !Arc::ptr_eq(&evidence.source, &failure.source)
            || evidence.reservation.is_some_and(|id| self.retains(id))
        {
            return false;
        }
        match evidence.outcome {
            AcquisitionOutcome::KnownNoNative => true,
            AcquisitionOutcome::Closed(receipt) => {
                evidence.reservation == Some(receipt.reservation())
            }
            AcquisitionOutcome::Unknown(_) => false,
        }
    }
}
