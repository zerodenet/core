//! The exact loan owner retains negotiated-control cleanup, independently of
//! its physical datagram origin. A read-only origin relation is not custody.
use super::*;
use crate::{
    contract::{lifecycle::CloseReport, ChannelError, DeliveryState, Reason, Stage},
    controlled_carrier::{error, CarrierAdoption},
};
use std::sync::Weak;

pub(in crate::controlled_carrier) struct RelatedControl {
    pub(super) parent: CarrierAdoption,
}
pub(super) struct Cleanup {
    pub token: Arc<RelatedControl>,
    pub protocol_closed: bool,
    pub pending: CloseReport,
}
impl DatagramLoan {
    /// Transfer this association's actual control original into the same loan
    /// owner's persistent cleanup. Only the previously linked same-owner pair
    /// qualifies. This neither changes physical ancestry nor proves closure.
    pub fn retain_related_control(&mut self, parent: CarrierAdoption) -> Result<(), ChannelError> {
        let actual = self.owner.original().related_adoption().ok_or_else(|| {
            error(
                parent.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            )
        })?;
        if !actual.same_slot(&parent) {
            return Err(error(
                parent.instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::NotSubmitted,
            ));
        }
        if let Some(stored) = &self.owner.related {
            if stored.token.parent.same_slot(&parent) {
                return Ok(());
            }
            return Err(error(
                parent.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        self.owner.related = Some(Box::new(Cleanup {
            token: Arc::new(RelatedControl { parent }),
            protocol_closed: false,
            pending: CloseReport::default(),
        }));
        Ok(())
    }
}
impl RelatedControl {
    pub(in crate::controlled_carrier) fn names(&self, pending: &CarrierAdoption) -> bool {
        self.parent.same_slot(pending)
    }
}
impl LoanOwner {
    pub(in crate::controlled_carrier) fn related_control(&self) -> Option<Weak<RelatedControl>> {
        self.related
            .as_ref()
            .map(|cleanup| Arc::downgrade(&cleanup.token))
    }
}
