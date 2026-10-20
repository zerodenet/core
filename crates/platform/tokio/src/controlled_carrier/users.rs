//! The same reservation's logical view and actual caller exit, never a second ledger.
use super::{error, slot::Slot, CarrierAdoption, CarrierGrant, CarrierOwnerRegistration};
use crate::contract::{
    scope::{TaskJoinWitness, TaskJoined},
    ChannelError, DeliveryState, Reason, Stage,
};
use std::sync::{atomic::Ordering, Arc};

#[derive(Clone, Default)]
pub(super) struct Users {
    issued: bool,
    live: bool,
    acquired: bool,
    caller: Option<TaskJoinWitness>,
}
/// Unique logical view lease. Return is one fact; it never issues a task exit.
pub struct CarrierUserView {
    slot: Arc<Slot>,
}
impl CarrierGrant {
    /// Claim before physical preparation. Move this exact lease into the actual
    /// logical read/write wrapper; split users retain that wrapper until both return.
    pub fn claim_user_view(&mut self) -> Result<CarrierUserView, ChannelError> {
        self.check()?;
        let mut state = self.slot.state();
        if state.closed
            || state.opening
            || state.spawn_attempted
            || state.users.as_ref().is_some_and(|users| users.issued)
        {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        state.users = Some(Users {
            issued: true,
            live: true,
            ..Default::default()
        });
        Ok(CarrierUserView {
            slot: self.slot.clone(),
        })
    }
}
impl CarrierUserView {
    pub fn matches(&self, adoption: &CarrierAdoption) -> bool {
        Arc::ptr_eq(&self.slot, &adoption.slot)
    }
}
impl Drop for CarrierUserView {
    fn drop(&mut self) {
        {
            if let Some(users) = &mut self.slot.state().users {
                users.live = false;
            }
        }
        self.slot.unused();
        self.slot.notify();
    }
}
impl Users {
    pub(super) fn bind(&mut self, witness: TaskJoinWitness) -> bool {
        if !self.issued {
            return false;
        }
        match &self.caller {
            Some(actual) => actual.same_registration(&witness),
            None => {
                self.caller = Some(witness);
                true
            }
        }
    }
    pub(super) fn live(&self) -> bool {
        self.live
    }
    pub(super) fn acquired(&self) -> bool {
        self.acquired
    }
    pub(super) fn caller(&self) -> Option<TaskJoinWitness> {
        self.caller.clone()
    }
    fn ready(&self) -> bool {
        !self.live
            && (!self.acquired || self.caller.as_ref().is_some_and(TaskJoinWitness::is_joined))
    }
}
pub(super) fn acquired(slot: &Arc<Slot>) {
    let tracked = {
        let mut state = slot.state();
        match &mut state.users {
            Some(users) => {
                users.acquired = true;
                users.issued && users.caller.is_some()
            }
            None => false,
        }
    };
    if let Some(owner) = slot.owner.upgrade() {
        owner.completion.acquired.store(true, Ordering::Release);
        if !tracked {
            owner
                .completion
                .users_unknown
                .store(true, Ordering::Release);
        }
    }
}
/// Opaque original slot proof: logical view returned, actual caller joined,
/// actual transport driver joined (when any), and original native control closed.
#[derive(Clone)]
pub struct CarrierUsersClosed {
    slot: Arc<Slot>,
    caller: Option<TaskJoinWitness>,
}
impl CarrierUsersClosed {
    pub fn matches(&self, adoption: &CarrierAdoption) -> bool {
        Arc::ptr_eq(&self.slot, &adoption.slot)
    }
    pub fn matches_caller(&self, caller: &TaskJoinWitness) -> bool {
        self.caller
            .as_ref()
            .is_some_and(|actual| actual.same_registration(caller))
    }
    /// None is explicitly the no-acquisition case, not a fabricated caller exit.
    pub fn caller_exit(&self) -> Option<TaskJoined> {
        self.caller.as_ref().and_then(TaskJoinWitness::try_joined)
    }
}
impl CarrierAdoption {
    pub fn users_closed(&self) -> Option<CarrierUsersClosed> {
        let (closed, users) = {
            let state = self.slot.state();
            (state.closed, state.users.clone())
        };
        let users = users?;
        if !closed || !users.ready() {
            return None;
        }
        Some(CarrierUsersClosed {
            slot: self.slot.clone(),
            caller: users.caller,
        })
    }
}
/// Issued only by explicit successful shutdown of the one actual owner after
/// every acquired original's caller/view/driver/native facts were confirmed.
#[derive(Clone)]
pub struct CarrierOwnerUsersClosed {
    completion: Arc<super::registration::Completion>,
}
impl CarrierOwnerUsersClosed {
    pub fn matches(&self, registration: &CarrierOwnerRegistration) -> bool {
        registration.matches_users_completion(&self.completion)
    }
    pub fn first_caller_failure(&self) -> Option<TaskJoined> {
        self.completion
            .caller_failure
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    pub fn had_native_acquisition(&self) -> bool {
        self.completion.acquired.load(Ordering::Acquire)
    }
}
impl CarrierOwnerRegistration {
    pub fn users_closed(&self) -> Option<CarrierOwnerUsersClosed> {
        let completion = self.users_completion();
        completion
            .users_closed
            .load(Ordering::Acquire)
            .then_some(CarrierOwnerUsersClosed { completion })
    }
}

/// Called only after the same original native control and transport driver have
/// really closed. No caller FD closure is awaited here: that would create a
/// cycle with the inbound owner's separate shutdown obligation.
pub(super) async fn finish(slot: &Arc<Slot>, deadline: tokio::time::Instant) -> bool {
    loop {
        let changed = slot.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let users = slot.state().users.clone();
        let Some(users) = users else {
            return true;
        };
        if users.live {
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return false;
            }
            continue;
        }
        if !users.acquired {
            return true;
        }
        let Some(caller) = users.caller else {
            // This old unaccounted path cannot issue a generation user receipt;
            // its sticky unknown bit survives physical record reclamation.
            return true;
        };
        let joined = match tokio::time::timeout_at(deadline, caller.wait()).await {
            Ok(joined) => joined,
            Err(_) => return false,
        };
        if joined.result().is_err() {
            if let Some(owner) = slot.owner.upgrade() {
                owner
                    .completion
                    .caller_failure
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get_or_insert(joined);
            }
        }
        return true;
    }
}
