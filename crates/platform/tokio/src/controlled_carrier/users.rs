//! The same reservation's logical view and actual caller exit, never a second ledger.
mod closed;
use super::{error, slot::Slot, CarrierAdoption, CarrierGrant, CarrierOwnerRegistration};
use crate::contract::{
    scope::{TaskJoinWitness, TaskJoined},
    ChannelError, DeliveryState, Reason, Stage,
};
pub use closed::{CarrierOwnerUsersClosed, CarrierUsersClosed};
use std::sync::{atomic::Ordering, Arc};

#[derive(Clone, Default)]
pub(super) struct Users {
    issued: bool,
    live: bool,
    acquired: bool,
    caller: Option<TaskJoinWitness>,
    driver_owned: bool,
    transferred_owned: bool,
    packet_driver:
        Option<crate::contract::component::packet_driver::attempt::PacketDriverPendingCustody>,
    pub(super) preparation: Option<Arc<super::preparation::Preparation>>,
}
/// Unique logical view lease. Return is one fact; it never issues a task exit.
pub struct CarrierUserView {
    pub(super) slot: Arc<Slot>,
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

    /// Unique taskless custody from an actual background owner or a declared
    /// node Datagram owner. Return is never a task exit or native-close proof.
    pub fn claim_transferred_view(&mut self) -> Result<CarrierUserView, ChannelError> {
        let bound = self
            .slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone());
        if !bound.is_some_and(|bound| bound.transferred_custody_available()) {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let view = self.claim_user_view()?;
        self.slot.state().users.as_mut().unwrap().transferred_owned = true;
        Ok(view)
    }
    /// A registered BackgroundControl carrier keeps its unique physical view
    /// in the owning protocol's one driver. Inline preparation may precede that
    /// driver; failed preparation has no invented caller task to join. Logical
    /// application streams remain the pool/host's separate retirement obligation.
    pub fn claim_driver_view(&mut self) -> Result<CarrierUserView, ChannelError> {
        let bound = self
            .slot
            .owner
            .upgrade()
            .and_then(|owner| owner.state().binding.clone());
        if !bound.is_some_and(|bound| bound.background_custody_available()) {
            return Err(error(
                self.instance(),
                Stage::Prepare,
                Reason::InvalidConfig,
                DeliveryState::NotSubmitted,
            ));
        }
        let view = self.claim_user_view()?;
        self.slot
            .state()
            .users
            .as_mut()
            .expect("unique view installed")
            .driver_owned = true;
        Ok(view)
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
    pub(super) fn caller_joined(&self) -> Option<TaskJoined> {
        if self.packet_driver_custody() {
            return None;
        }
        if let Some(preparation) = &self.preparation {
            if !preparation.published.load(Ordering::Acquire) {
                // Its real Auxiliary error has its own local acceptance and
                // original Scope receipt. It is not a Business caller fault.
                return None;
            }
        }
        self.caller.as_ref().and_then(TaskJoinWitness::try_joined)
    }

    pub(super) fn driver_custody_live(&self) -> bool {
        self.driver_owned
            && self.live
            && self.preparation.as_ref().is_none_or(|preparation| {
                preparation.published.load(Ordering::Acquire) && preparation.job.get().is_none()
            })
    }

    pub(super) fn protocol_target_custody(&self, cached: bool) -> bool {
        if cached {
            self.driver_owned
        } else {
            self.transferred_owned
        }
    }
    pub(super) fn transferred(&self) -> bool {
        self.transferred_owned
    }
    /// Exact pre-native transferred Service preparation. Never binds a
    /// Business caller, changes custody class or accepts another same-Scope task.
    pub(super) fn preparing_transferred_for(&self, caller: &TaskJoinWitness) -> bool {
        self.issued
            && self.live
            && self.transferred_owned
            && !self.driver_owned
            && !self.acquired
            && self.caller.is_none()
            && self.preparation.as_ref().is_some_and(|preparation| {
                preparation.expected_caller.is_none()
                    && !preparation.published.load(Ordering::Acquire)
                    && preparation
                        .auxiliary
                        .get()
                        .is_some_and(|actual| actual.same_registration(caller))
            })
    }
    pub(super) fn install_packet_driver_custody(
        &mut self,
        proof: crate::contract::component::packet_driver::attempt::PacketDriverPendingCustody,
    ) -> bool {
        if !self.issued {
            return false;
        }
        // A real callee can already own this exact native original through its
        // declared driver/transferred preparation. Do not reclassify that
        // existing custody or bind its actual Auxiliary to the Source wire.
        if self.driver_owned || self.transferred_owned {
            return true;
        }
        if self
            .caller
            .as_ref()
            .is_some_and(|caller| !proof.matches_caller(caller))
            || self
                .preparation
                .as_ref()
                .and_then(|p| p.expected_caller.as_ref())
                .is_some_and(|caller| !proof.matches_caller(caller))
            || (self.acquired && self.caller.is_none())
        {
            return false;
        }
        if let Some(actual) = &self.packet_driver {
            return actual.same_record(&proof);
        }
        self.packet_driver = Some(proof);
        true
    }
    pub(super) fn packet_driver_custody(&self) -> bool {
        self.packet_driver
            .as_ref()
            .is_some_and(|proof| proof.retained())
    }
    pub(super) fn bind(&mut self, witness: TaskJoinWitness) -> bool {
        if !self.issued
            || self.driver_owned
            || self.transferred_owned
            || self
                .packet_driver
                .as_ref()
                .is_some_and(|proof| !proof.matches_caller(&witness))
        {
            return false;
        }
        if self.preparation.as_ref().is_some_and(|preparation| {
            !preparation
                .expected_caller
                .as_ref()
                .is_some_and(|actual| actual.same_registration(&witness))
        }) {
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
    pub(super) fn caller_matches(&self, caller: &TaskJoinWitness) -> bool {
        self.caller
            .as_ref()
            .is_some_and(|actual| actual.same_registration(caller))
    }
    pub(super) fn credit_caller(&self) -> Option<TaskJoinWitness> {
        if let Some(preparation) = &self.preparation {
            if !preparation.published.load(Ordering::Acquire) {
                return preparation.auxiliary.get().cloned();
            }
        }
        if self.acquired
            && !self.driver_owned
            && !self.transferred_owned
            && !self.packet_driver_custody()
        {
            self.caller.clone()
        } else {
            None
        }
    }
    pub(super) fn ready(&self) -> bool {
        !self.live
            && (!self.acquired
                || self.driver_owned
                || self.transferred_owned
                || self.packet_driver_custody()
                || match &self.preparation {
                    Some(preparation) if !preparation.published.load(Ordering::Acquire) => {
                        preparation
                            .auxiliary
                            .get()
                            .is_some_and(TaskJoinWitness::is_joined)
                    }
                    _ => self.caller.as_ref().is_some_and(TaskJoinWitness::is_joined),
                })
    }
}
pub(super) fn acquired(slot: &Arc<Slot>) {
    let tracked = {
        let mut state = slot.state();
        match &mut state.users {
            Some(users) => {
                users.acquired = true;
                users.issued
                    && (users.driver_owned || users.transferred_owned || users.caller.is_some())
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
        if let Some(preparation) = &users.preparation {
            if !preparation.published.load(Ordering::Acquire) {
                let Some(auxiliary) = preparation.auxiliary.get() else {
                    return false;
                };
                return tokio::time::timeout_at(deadline, auxiliary.wait())
                    .await
                    .is_ok_and(|joined| auxiliary.matches_joined(&joined));
            }
        }
        if users.driver_owned || users.transferred_owned || users.packet_driver_custody() {
            // close_slot reached this point only after the same original
            // native close (and the driver's actual join when present). Its unique
            // custody view returned above; there is no separate caller task.
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
