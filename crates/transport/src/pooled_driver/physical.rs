use super::{failure, ManagedPool};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
use zero_platform_tokio::{
    contract::{ChannelError, Reason, Stage},
    controlled_carrier::CarrierAdoption,
};

pub struct Physical {
    pub(super) original: CarrierAdoption,
    pub(super) owner: Weak<ManagedPool>,
    pub(super) opening: AtomicBool,
    pub(super) done: AtomicBool,
    pub(super) registered: AtomicBool,
    pub(super) stop: zero_platform_tokio::contract::CancellationToken,
    users: AtomicUsize,
    pub(super) witness: Mutex<Option<zero_platform_tokio::contract::scope::TaskJoinWitness>>,
    received: Mutex<Option<zero_platform_tokio::contract::scope::TaskJoined>>,
    accepted: AtomicBool,
}
impl Physical {
    pub(super) fn new(original: CarrierAdoption, owner: &Arc<ManagedPool>) -> Arc<Self> {
        Arc::new(Self {
            original,
            owner: Arc::downgrade(owner),
            opening: AtomicBool::new(true),
            done: AtomicBool::new(false),
            registered: AtomicBool::new(false),
            stop: owner.cancel.child_token(),
            users: AtomicUsize::new(0),
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
        self.users
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_add(1))
            .map_err(|_| failure(self.original.instance(), Stage::Prepare, Reason::Capacity))?;
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
    pub(super) fn closed(&self) -> bool {
        if self.opening.load(Ordering::Acquire)
            || self.users.load(Ordering::Acquire) != 0
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
            (Some(witness), Some(joined)) => witness.matches_joined(&joined),
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
            self.0.done.store(true, Ordering::Release);
        }
        if let Some(owner) = self.0.owner.upgrade() {
            owner.changed.notify_waiters();
        }
    }
}
pub(super) struct Body(pub(super) Arc<Physical>);
impl Drop for Body {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
        if let Some(owner) = self.0.owner.upgrade() {
            owner.changed.notify_waiters();
        }
    }
}
