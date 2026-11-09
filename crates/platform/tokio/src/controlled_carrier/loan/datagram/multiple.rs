//! Multiple targets remain explicit packet addresses, never a byte-only relay.
use super::*;
use crate::contract::{ChannelError, DeliveryState, Reason, Stage};

impl DatagramLoan {
    /// Preserve the actual session's Multiple capability and unique owner.
    /// `wire_relay` is observational; it grants no payload target permission.
    /// A rejected conversion returns the same loan for actual cleanup custody.
    pub fn new_multiple(
        session: BoxDatagramSession,
        original: CarrierOrigin,
        association: AssociationId,
        wire_relay: Destination,
    ) -> Result<Self, (ChannelError, Self)> {
        Self::new(session, original, association, wire_relay).into_multiple_targets()
    }

    /// Select the existing session's target-aware interface, without changing
    /// its limits, controller, association, original or registered task graph.
    pub fn into_multiple_targets(mut self) -> Result<Self, (ChannelError, Self)> {
        if !matches!(self.io.limits.targets, DatagramTargets::Multiple) {
            let error = crate::controlled_carrier::error(
                self.owner.original().instance(),
                Stage::Prepare,
                Reason::StalePlan,
                DeliveryState::Unknown,
            );
            return Err((error, self));
        }
        self.io.multiple_targets = true;
        Ok(self)
    }
}
impl LoanDatagramIo {
    pub(super) fn require_fixed_target(&self) -> io::Result<()> {
        if self.multiple_targets {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Multiple datagram loan requires target-aware Tx/Rx",
            ))
        } else {
            Ok(())
        }
    }
}
