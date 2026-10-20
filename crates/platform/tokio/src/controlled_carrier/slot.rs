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
    pub(super) opening: bool,
    pub(super) view_live: bool,
    pub(super) spawning: bool,
    pub(super) spawn_attempted: bool,
    pub(super) driver: Option<(TaskId, TaskJoinWitness)>,
    pub(super) control: Option<Arc<AsyncMutex<ControlledTcpControl>>>,
    pub(super) close_observer: Option<crate::controlled_tcp::ControlledTcpCloseObserver>,
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
                && state.unknown.is_none()
                && !state.opening
                && !state.view_live
                && !state.spawning
                && state.driver.is_none()
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
