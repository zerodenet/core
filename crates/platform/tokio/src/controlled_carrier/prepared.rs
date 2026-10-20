use super::{adoption::CarrierAdoption, connect_error, error, slot::Slot, view::CarrierIo};
use crate::{
    contract::{CancellationToken, ChannelError, DeliveryState, Reason, Stage},
    EgressInterfaceControl, PendingTcpConnect,
};
use std::sync::Arc;
use tokio::time::Instant;
use zero_core::component::NetworkGeneration;
/// The persistent owner already holds the original native control. Dropping an
/// unpolled or pending open retires the slot; it never drops the sole control.
pub struct PreparedCarrier {
    pub(super) pending: PendingTcpConnect,
    pub(super) slot: Arc<Slot>,
    pub(super) egress: Arc<EgressInterfaceControl>,
    pub(super) generation: NetworkGeneration,
    pub(super) cancel: CancellationToken,
    pub(super) end: Instant,
    pub(super) transferred: bool,
}
pub struct ControlledCarrier {
    io: CarrierIo,
    adoption: CarrierAdoption,
}
impl ControlledCarrier {
    pub fn into_parts(self) -> (CarrierIo, CarrierAdoption) {
        (self.io, self.adoption)
    }
    pub fn adoption(&self) -> &CarrierAdoption {
        &self.adoption
    }
}
impl PreparedCarrier {
    pub async fn open(mut self, deadline: Instant) -> Result<ControlledCarrier, ChannelError> {
        if self.egress.generation() != self.generation.0 {
            return Err(error(
                self.slot.instance,
                Stage::Dial,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        let end = self.end.min(deadline);
        let socket = tokio::select! {biased;
            _ = self.cancel.cancelled() => return Err(error(self.slot.instance, Stage::Dial, Reason::Cancelled, DeliveryState::Unknown)),
            result = self.pending.connect(end) => result.map_err(|source|connect_error(self.slot.instance, source))?,
        };
        if self.slot.cancel.is_cancelled() || self.cancel.is_cancelled() {
            return Err(error(
                self.slot.instance,
                Stage::Dial,
                Reason::Cancelled,
                DeliveryState::Unknown,
            ));
        }
        if self.egress.generation() != self.generation.0 {
            return Err(error(
                self.slot.instance,
                Stage::Dial,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        let mut state = self.slot.state();
        state.view_live = true;
        let carrier = ControlledCarrier {
            io: CarrierIo {
                io: Box::pin(socket),
                slot: self.slot.clone(),
            },
            adoption: CarrierAdoption {
                slot: self.slot.clone(),
            },
        };
        drop(state);
        self.transferred = true;
        Ok(carrier)
    }
}
impl Drop for PreparedCarrier {
    fn drop(&mut self) {
        self.slot.state().opening = false;
        if !self.transferred {
            self.slot.cancel.cancel();
        }
        self.slot.notify();
    }
}
