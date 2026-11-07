//! The unique actual borrowed IO/view binds its real endpoint's existing
//! driver/user ledgers. No native, cached-driver or additional Scope rights.
use super::BorrowedDatagram;
use crate::{
    contract::{
        scope::{TaskRegistrar, TaskScopeIdentity},
        ChannelError, DeliveryState, Reason, Stage,
    },
    controlled_carrier::{CarrierAdoption, CarrierUserView},
    controlled_datagram::metadata::users::{
        drivers::{
            Admission, DriverExitReceiver, DriverKind, DriverOwner, DriverRegistrationsClosed,
            RegistrationIter,
        },
        LogicalUsers,
    },
};
use std::{
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Weak,
    },
};
pub(in crate::controlled_carrier) struct Identity {
    slot: Weak<crate::controlled_carrier::slot::Slot>,
    scope: TaskScopeIdentity,
    bound: AtomicBool,
}
pub struct CarrierEndpointClaim(Arc<Identity>);
struct Graph<O: DriverKind, B: Send + Sync + 'static> {
    original: Arc<Identity>,
    drivers: DriverOwner<O, B>,
    users: Arc<LogicalUsers<Admission<O, B>>>,
}
pub struct CarrierEndpointUserGraph<O: DriverKind, B: Send + Sync + 'static>(Arc<Graph<O, B>>);
pub struct CarrierEndpointUsersCompletion<O: DriverKind, B: Send + Sync + 'static>(
    Arc<Graph<O, B>>,
);
#[derive(Clone)]
pub struct CarrierEndpointUsersClosed {
    original: Arc<Identity>,
    registrations: Option<DriverRegistrationsClosed>,
}
fn failure() -> ChannelError {
    let mut error = ChannelError::new(
        Stage::Close,
        Reason::StalePlan,
        crate::contract::Impact::Component,
    );
    error.delivery = DeliveryState::Unknown;
    error
}
impl BorrowedDatagram {
    /// Before Endpoint construction, once for this actual unique IO and view.
    /// Returning the original on refusal leaves all native/loan debt in place.
    pub fn claim_endpoint_users(
        &self,
        view: &CarrierUserView,
        tasks: &TaskRegistrar,
    ) -> Result<CarrierEndpointClaim, ChannelError> {
        let slot = self.adoption().slot;
        if !view.matches(&self.adoption()) {
            return Err(failure());
        }
        let bound = slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone())
            .ok_or_else(failure)?;
        if !bound.permits_datagram_protocol_control(tasks) {
            return Err(failure());
        }
        let _closing = slot.closing.try_lock().map_err(|_| failure())?;
        let mut state = slot.state();
        if state.closed
            || state.protocol.is_none()
            || state.loan.is_none()
            || state.endpoint_claim.is_some()
            || state.spawn_attempted
            || state.spawning
            || !state
                .users
                .as_ref()
                .is_some_and(|users| users.live() && users.acquired() && users.transferred())
        {
            return Err(failure());
        }
        let identity = Arc::new(Identity {
            slot: Arc::downgrade(&slot),
            scope: tasks.scope_identity(),
            bound: AtomicBool::new(false),
        });
        state.endpoint_claim = Some(identity.clone());
        Ok(CarrierEndpointClaim(identity))
    }
}
impl CarrierEndpointClaim {
    /// Actual owner installs these same ledgers into its real Quinn hooks and
    /// user permits. There is no independent map/counter to reconcile later.
    pub fn bind_users<O: DriverKind, B: Send + Sync + 'static>(
        self,
        tasks: TaskRegistrar,
        max_drivers: NonZeroUsize,
        max_users: NonZeroUsize,
    ) -> Result<(CarrierEndpointUserGraph<O, B>, DriverExitReceiver<O, B>), Self> {
        if !self.0.scope.matches(&tasks) || !self.0.scope.registration_open() {
            return Err(self);
        }
        let Some(slot) = self.0.slot.upgrade() else {
            return Err(self);
        };
        let bound = slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone());
        if slot.cancel.is_cancelled()
            || !bound.is_some_and(|bound| bound.permits_datagram_protocol_control(&tasks))
        {
            return Err(self);
        }
        let Ok(closing) = slot.closing.try_lock() else {
            return Err(self);
        };
        let state = slot.state();
        if state.closed
            || state.loan.is_none()
            || state.protocol.is_none()
            || !state
                .endpoint_claim
                .as_ref()
                .is_some_and(|actual| Arc::ptr_eq(actual, &self.0))
            || self
                .0
                .bound
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            drop(state);
            drop(closing);
            return Err(self);
        }
        drop(state);
        drop(closing);
        let cancel = tasks.cancellation();
        let (drivers, receiver) = DriverOwner::new(tasks, max_drivers);
        let users = LogicalUsers::new(max_users, cancel, drivers.admission());
        Ok((
            CarrierEndpointUserGraph(Arc::new(Graph {
                original: self.0,
                drivers,
                users,
            })),
            receiver,
        ))
    }
    /// No endpoint ever received this move-only claim. Native/view/packet
    /// driver return is still an independent obligation of its same Slot.
    pub fn return_unstarted(self) -> Result<CarrierEndpointUsersClosed, Self> {
        if self.0.bound.load(Ordering::Acquire) {
            return Err(self);
        }
        Ok(CarrierEndpointUsersClosed {
            original: self.0,
            registrations: None,
        })
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> CarrierEndpointUserGraph<O, B> {
    pub fn drivers(&self) -> &DriverOwner<O, B> {
        &self.0.drivers
    }
    pub fn users(&self) -> &Arc<LogicalUsers<Admission<O, B>>> {
        &self.0.users
    }
    pub fn completion(&self) -> CarrierEndpointUsersCompletion<O, B> {
        CarrierEndpointUsersCompletion(self.0.clone())
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> Clone for CarrierEndpointUsersCompletion<O, B> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<O: DriverKind, B: Send + Sync + 'static> CarrierEndpointUsersCompletion<O, B> {
    pub fn try_owned_closed(&self) -> Option<CarrierEndpointUsersClosed> {
        if !self.0.users.cancellation().is_cancelled() || self.0.users.active() != 0 {
            return None;
        }
        Some(CarrierEndpointUsersClosed {
            original: self.0.original.clone(),
            registrations: Some(self.0.drivers.owned_registration_completion()?),
        })
    }
}
impl CarrierEndpointUsersClosed {
    pub fn matches(&self, actual: &CarrierAdoption) -> bool {
        self.original.slot.ptr_eq(&Arc::downgrade(&actual.slot))
    }
    pub fn registrations(&self) -> Option<RegistrationIter> {
        self.registrations
            .as_ref()
            .map(DriverRegistrationsClosed::registrations)
    }
    pub(in crate::controlled_carrier) fn acknowledged(&self) -> bool {
        self.registrations().is_none_or(|mut registrations| {
            registrations.all(|actual| {
                actual.try_joined().is_some_and(|joined| {
                    actual.matches_joined(&joined) && joined.registration_released()
                })
            })
        })
    }
}
impl CarrierAdoption {
    /// Retain the same original graph receipt before NativeClosed. Its actual
    /// Host ACK is checked only by original Slot/N removal, never native close.
    pub fn retain_endpoint_users_closed(
        &self,
        receipt: CarrierEndpointUsersClosed,
    ) -> Result<(), ChannelError> {
        if !receipt.matches(self) {
            return Err(failure());
        }
        let mut state = self.slot.state();
        if !state
            .endpoint_claim
            .as_ref()
            .is_some_and(|original| Arc::ptr_eq(original, &receipt.original))
        {
            return Err(failure());
        }
        if let Some(previous) = &state.endpoint_closed {
            if !Arc::ptr_eq(&previous.original, &receipt.original) {
                return Err(failure());
            }
        } else {
            state.endpoint_closed = Some(receipt);
        }
        drop(state);
        self.slot.notify();
        Ok(())
    }
}
