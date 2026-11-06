use super::{failure, ManagedPool};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
use zero_platform_tokio::{
    contract::{ChannelError, Reason, Stage},
    controlled_carrier::{CarrierAdoption, CarrierUserView},
};

pub struct Physical {
    pub(super) original: CarrierAdoption,
    pub(super) owner: Weak<ManagedPool>,
    pub(super) opening: AtomicBool,
    pub(super) done: AtomicBool,
    pub(super) registered: AtomicBool,
    pub(super) stop: zero_platform_tokio::contract::CancellationToken,
    users: AtomicUsize,
    view: Mutex<Option<CarrierUserView>>,
    pub(super) witness: Mutex<Option<zero_platform_tokio::contract::scope::TaskJoinWitness>>,
    received: Mutex<Option<zero_platform_tokio::contract::scope::TaskJoined>>,
    accepted: AtomicBool,
}
impl Physical {
    pub(super) fn new(
        original: CarrierAdoption,
        stop: zero_platform_tokio::contract::CancellationToken,
        view: CarrierUserView,
        owner: &Arc<ManagedPool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            original,
            owner: Arc::downgrade(owner),
            opening: AtomicBool::new(true),
            done: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            stop,
            users: AtomicUsize::new(0),
            view: Mutex::new(Some(view)),
            witness: Mutex::new(None),
            received: Mutex::new(None),
            accepted: AtomicBool::new(false),
        })
    }
    pub fn user(self: &Arc<Self>) -> Result<PoolUse, ChannelError> {
        let owner = self
            .owner
            .upgrade()
            .filter(|owner| owner.ready() && self.usable())
            .ok_or_else(|| failure(self.original.instance(), Stage::Prepare, Reason::Revoked))?;
        let mut current = self.users.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(1).ok_or_else(|| {
                failure(self.original.instance(), Stage::Prepare, Reason::Capacity)
            })?;
            match self.users.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        // Stop can race this acquisition. Keep its debt until the returned use
        // is actually dropped, even when admission has just closed.
        let usage = PoolUse {
            _use: Arc::new(Use {
                record: self.clone(),
            }),
        };
        if !owner.ready() || !self.usable() {
            return Err(failure(
                self.original.instance(),
                Stage::Prepare,
                Reason::Revoked,
            ));
        }
        Ok(usage)
    }
    // An ended/stopped body denies new users; these flags never certify close.
    fn usable(&self) -> bool {
        self.registered.load(Ordering::Acquire)
            && !self.done.load(Ordering::Acquire)
            && !self.stop.is_cancelled()
    }
    // The one physical driver lease remains in this exact record until the
    // driver body ended AND every logical stream/control returned. Native close
    // can precede that fact; platform user/N settlement cannot.
    fn release_finished_view(&self) {
        if self.done.load(Ordering::Acquire) && self.users.load(Ordering::Acquire) == 0 {
            let returned = { self.view.lock().unwrap().take() };
            drop(returned);
        }
    }
    pub(super) fn closed(&self) -> bool {
        if self.opening.load(Ordering::Acquire)
            || self.users.load(Ordering::Acquire) != 0
            || self.view.lock().unwrap().is_some()
            || self
                .original
                .auxiliary_exit()
                .is_some_and(|joined| !joined.registration_released())
            || !self.failure_accepted()
        {
            return false;
        }
        let Some(closed) = self
            .original
            .closed()
            .filter(|proof| proof.matches(&self.original))
        else {
            return false;
        };
        if !self
            .original
            .users_closed()
            .is_some_and(|proof| proof.matches(&self.original))
        {
            return false;
        }
        match (self.witness.lock().unwrap().as_ref(), closed.driver_exit()) {
            (None, None) => !self.registered.load(Ordering::Acquire),
            (Some(witness), Some(joined)) => {
                witness.matches_joined(&joined)
                    && witness
                        .try_joined()
                        .is_some_and(|received| received.registration_released())
            }
            _ => false,
        }
    }
    // Local failure delivery stays in this physical record. The host's actual
    // publication acknowledgement remains separate; no synthetic exit is made.
    pub(super) fn receive_available(&self) {
        let witness = self.witness.lock().unwrap().clone();
        if let Some(witness) = witness {
            if let Some(joined) = witness.try_joined() {
                let received = self.received.lock().unwrap().get_or_insert(joined).clone();
                if received.result().is_err()
                    && !self.accepted.load(Ordering::Acquire)
                    && witness.acknowledge_hosted_failure(&received).is_ok()
                {
                    self.accepted.store(true, Ordering::Release);
                }
            }
        }
    }
    fn failure_accepted(&self) -> bool {
        self.witness
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|witness| witness.try_joined())
            .is_none_or(|joined| joined.result().is_ok() || self.accepted.load(Ordering::Acquire))
    }
    pub fn origin(
        &self,
    ) -> Result<zero_platform_tokio::controlled_carrier::CarrierOrigin, ChannelError> {
        let witness =
            self.witness.lock().unwrap().clone().ok_or_else(|| {
                failure(self.original.instance(), Stage::Prepare, Reason::StalePlan)
            })?;
        self.original.origin(&witness)
    }
    pub fn instance(&self) -> zero_core::component::ComponentInstanceId {
        self.original.instance()
    }
}
/// Lightweight shared custody only; dropping this never issues native proof.
#[derive(Clone)]
pub struct PoolUse {
    _use: Arc<Use>,
}
struct Use {
    record: Arc<Physical>,
}
impl Drop for Use {
    fn drop(&mut self) {
        self.record.users.fetch_sub(1, Ordering::AcqRel);
        self.record.release_finished_view();
        if let Some(owner) = self.record.owner.upgrade() {
            owner.changed.notify_waiters();
        }
    }
}
pub(super) struct Opening(pub(super) Arc<Physical>);
impl Drop for Opening {
    fn drop(&mut self) {
        self.0.opening.store(false, Ordering::Release);
        if !self.0.registered.load(Ordering::Acquire) {
            self.0.original.request_retirement();
            self.0.done.store(true, Ordering::Release);
            self.0.release_finished_view();
        }
        if let Some(owner) = self.0.owner.upgrade() {
            owner.changed.notify_waiters();
        }
    }
}
pub(super) struct Body(pub(super) Arc<Physical>);
impl Drop for Body {
    fn drop(&mut self) {
        self.0.original.request_retirement();
        self.0.done.store(true, Ordering::Release);
        self.0.release_finished_view();
        if let Some(owner) = self.0.owner.upgrade() {
            owner.changed.notify_waiters();
        }
    }
}
