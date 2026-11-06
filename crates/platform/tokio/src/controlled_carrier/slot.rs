use super::owner::Shared;
use crate::{
    contract::{scope::TaskJoinWitness, ChannelError},
    ControlledTcpControl,
};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio_util::sync::CancellationToken;
use zero_core::component::{ComponentInstanceId, TaskId};
pub(super) struct Slot {
    pub(super) completion: Arc<std::sync::OnceLock<bool>>,
    pub(super) instance: ComponentInstanceId,
    pub(super) ordinal: u64,
    pub(super) owner: Weak<Shared>,
    pub(super) cancel: CancellationToken,
    pub(super) state: Mutex<State>,
    pub(super) changed: Notify,
    pub(super) closing: AsyncMutex<()>,
}
#[derive(Default)]
pub(super) struct State {
    pub(super) related_parent: Option<Weak<Slot>>,
    pub(super) related_child: Option<Arc<Slot>>,
    pub(super) transferred_origin: bool,
    pub(super) loan: Option<Arc<AsyncMutex<super::loan::LoanOwner>>>,
    pub(super) loan_origin: Option<super::CarrierOrigin>,
    pub(super) loan_related: Option<Weak<super::loan::RelatedControl>>,
    pub(super) binding: Option<crate::contract::network::UpstreamBinding>,
    pub(super) endpoint: Option<zero_core::request::Destination>,
    pub(super) opening: bool,
    pub(super) view_live: bool,
    pub(super) endpoints: Option<super::CarrierEndpoints>,
    pub(super) business_cancel: Option<CancellationToken>,
    pub(super) service_identity: Option<Weak<super::service::ServiceDemand>>,
    pub(super) business_identity: Option<Weak<dyn super::CarrierBusinessDemand>>,
    pub(super) users: Option<super::users::Users>,
    pub(super) pending_close: Option<crate::contract::lifecycle::CloseReport>,
    pub(super) spawning: bool,
    pub(super) spawn_attempted: bool,
    pub(super) driver: Option<(TaskId, TaskJoinWitness)>,
    pub(super) auxiliary_attempted: bool,
    pub(super) auxiliary: Option<TaskJoinWitness>,
    pub(super) auxiliary_exit: Option<crate::contract::scope::TaskJoined>,
    pub(super) auxiliary_accepted: bool,
    pub(super) control: Option<Arc<AsyncMutex<ControlledTcpControl>>>,
    pub(super) close_observer: Option<crate::controlled_tcp::ControlledTcpCloseObserver>,
    pub(super) packet: Option<Arc<crate::controlled_datagram::ControlledDatagramControl>>,
    pub(super) packet_disposition: Option<crate::controlled_datagram::AcquisitionOutcome>,
    pub(super) packet_owner:
        Option<Arc<Mutex<crate::controlled_datagram::ControlledDatagramOwner>>>,
    /// Synchronous preparation acquired/consumed a handle without a control.
    pub(super) unknown: Option<Arc<ChannelError>>,
    pub(super) closed: bool,
}
impl Slot {
    pub(super) fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }
    pub(super) fn label(&self) -> String {
        format!(
            "controlled TCP carrier {:?}/{}",
            self.instance, self.ordinal
        )
    }
    pub(super) fn notify(&self) {
        self.changed.notify_waiters();
    }
    pub(super) fn remove(self: &Arc<Self>) {
        let state = self.state();
        assert!(
            state.closed,
            "only confirmed original retirement removes a slot"
        );
        let native = state.control.is_some() || state.packet.is_some();
        let _ = self.completion.set(native);
        drop(state);
        if let Some(owner) = self.owner.upgrade() {
            owner
                .state()
                .slots
                .retain(|stored| !Arc::ptr_eq(stored, self));
        }
    }
    pub(super) fn unused(self: &Arc<Self>) {
        let unused = {
            let mut state = self.state();
            if state.control.is_none()
                && state.loan.is_none()
                && state.packet.is_none()
                && state.unknown.is_none()
                && !state.opening
                && !state.view_live
                && !state.spawning
                && state.driver.is_none()
                && state.auxiliary.is_none()
                && state
                    .users
                    .as_ref()
                    .is_none_or(|users| !users.live() && !users.acquired())
            {
                state.closed = true;
                true
            } else {
                false
            }
        };
        if unused {
            self.remove();
        }
        self.notify();
    }
}
