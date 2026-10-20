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
    pub(super) authorization: Option<CancellationToken>,
    pub(super) end: Instant,
    pub(super) endpoint_observer: Option<Arc<dyn super::CarrierEndpointObserver>>,
    pub(super) session: Option<zero_core::component::SessionId>,
    pub(super) business: Option<super::CarrierBusinessGrant>,
    pub(super) transferred: bool,
}
pub struct ControlledCarrier {
    io: CarrierIo,
    adoption: CarrierAdoption,
    endpoints: super::CarrierEndpoints,
}
impl ControlledCarrier {
    pub fn endpoints(&self) -> &super::CarrierEndpoints {
        &self.endpoints
    }
    pub fn into_parts(self) -> (CarrierIo, CarrierAdoption) {
        (self.io, self.adoption)
    }
    pub fn adoption(&self) -> &CarrierAdoption {
        &self.adoption
    }
}
impl PreparedCarrier {
    pub fn with_endpoint_observer(
        mut self,
        observer: Arc<dyn super::CarrierEndpointObserver>,
    ) -> Self {
        self.endpoint_observer = Some(observer);
        self
    }
    pub async fn open(self, deadline: Instant) -> Result<ControlledCarrier, ChannelError> {
        self.open_with_deadline(Some(deadline)).await
    }
    /// None preserves the original native operation end, not an invented total
    /// business handshake deadline. Explicit callers can only shorten that end.
    pub async fn open_with_deadline(
        self,
        deadline: Option<Instant>,
    ) -> Result<ControlledCarrier, ChannelError> {
        let session = self.session;
        self.open_inner(deadline).await.map_err(|mut error| {
            if error.session.is_none() {
                error.session = session;
            }
            error
        })
    }
    async fn open_inner(
        mut self,
        deadline: Option<Instant>,
    ) -> Result<ControlledCarrier, ChannelError> {
        if let Some(business) = &self.business {
            business.check_liveness()?;
        }
        if self.egress.generation() != self.generation.0 {
            return Err(error(
                self.slot.instance,
                Stage::Dial,
                Reason::StalePlan,
                DeliveryState::Unknown,
            ));
        }
        let end = deadline.map_or(self.end, |deadline| self.end.min(deadline));
        let business_cancel = self
            .business
            .as_ref()
            .map(|business| business.cancellation());
        let cancelled = async {
            match &business_cancel {
                Some(cancel) => cancel.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let authorization = self.authorization.clone();
        let revoked = async {
            match &authorization {
                Some(cancel) => cancel.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let socket = tokio::select! {biased;
            _ = cancelled => return Err(error(self.slot.instance, Stage::Dial, Reason::Cancelled, DeliveryState::Unknown)),
            _ = revoked => return Err(error(self.slot.instance, Stage::Dial, Reason::Revoked, DeliveryState::Unknown)),
            _ = self.cancel.cancelled() => return Err(error(self.slot.instance, Stage::Dial, Reason::Cancelled, DeliveryState::Unknown)),
            result = self.pending.connect(end) => result.map_err(|source|connect_error(self.slot.instance, source))?,
        };
        if self.slot.cancel.is_cancelled()
            || self.cancel.is_cancelled()
            || self
                .authorization
                .as_ref()
                .is_some_and(CancellationToken::is_cancelled)
        {
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
        if let Some(business) = &self.business {
            business.check_liveness().map_err(|mut failure| {
                failure.delivery = DeliveryState::Unknown;
                failure
            })?;
        }
        // Read real socket facts before boxing; original native control is
        // already persistent. No enclosing carrier lock surrounds these getters.
        let endpoints = super::CarrierEndpoints {
            origin: Arc::downgrade(&self.slot),
            local: Some(socket.local_addr().map_err(|source| {
                error(
                    self.slot.instance,
                    Stage::Dial,
                    Reason::Io,
                    DeliveryState::Unknown,
                )
                .with_source(source)
            })?),
            peer: socket.peer_addr().map_err(|source| {
                error(
                    self.slot.instance,
                    Stage::Dial,
                    Reason::Io,
                    DeliveryState::Unknown,
                )
                .with_source(source)
            })?,
        };
        {
            let mut state = self.slot.state();
            state.view_live = true;
            state.endpoints = Some(endpoints.clone());
        }
        let carrier = ControlledCarrier {
            io: CarrierIo {
                io: Box::pin(socket),
                slot: self.slot.clone(),
            },
            adoption: CarrierAdoption {
                slot: self.slot.clone(),
            },
            endpoints,
        };
        // CarrierIo now owns the view-live guard: callback panic/drop restores
        // view accounting while retaining the original native control.
        if let Some(observer) = &self.endpoint_observer {
            observer.connected(&carrier.endpoints);
        }
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
