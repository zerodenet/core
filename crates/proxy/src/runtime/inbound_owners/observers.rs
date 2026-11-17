//! A single persistent parking place for the original listener observer tasks.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinSet;
mod wake;
use wake::{ObserverWake, WaitTarget};

#[cfg(feature = "raw-ip-runtime")]
type SystemTunProjection = crate::inbound::tun::exit::SystemTunExitProjection;
#[cfg(not(feature = "raw-ip-runtime"))]
type SystemTunProjection = ();

mod join;
use join::ObserverResult;
type Observers = JoinSet<ObserverResult>;
struct SystemTunObserver {
    owner: Arc<InboundOwner>,
    exit: Option<SystemTunProjection>,
}

/// The registry owns exactly one JoinSet. An orchestration may borrow its
/// ownership, but cancellation returns it instead of aborting its actual tasks.
pub(super) struct ObserverParking {
    parked: Mutex<Option<ObserverBatch>>,
    needs_retirement: AtomicBool,
    // Original API Source observers awaiting the same exclusive JoinSet.
    // Their native/task custody stays in the existing registry, never here.
    system_tun: Mutex<Vec<SystemTunObserver>>,
    changed: tokio::sync::Notify,
}
impl Default for ObserverParking {
    fn default() -> Self {
        Self {
            parked: Mutex::new(Some(ObserverBatch {
                tasks: JoinSet::new(),
                wake: Arc::default(),
                failure: None,
            })),
            needs_retirement: AtomicBool::new(false),
            system_tun: Mutex::new(Vec::new()),
            changed: tokio::sync::Notify::new(),
        }
    }
}

struct ObserverBatch {
    tasks: Observers,
    wake: Arc<ObserverWake>,
    // One current-generation fault, retained across interrupted retirement.
    failure: Option<EngineError>,
}

/// Non-clone exclusive lease; no additional task, Scope or join consumer.
pub(crate) struct ListenerObservers {
    parking: Arc<ObserverParking>,
    owned: Option<ObserverBatch>,
    recovered: bool,
}
impl Deref for ListenerObservers {
    type Target = Observers;
    fn deref(&self) -> &Observers {
        &self
            .owned
            .as_ref()
            .expect("live observer ownership lease")
            .tasks
    }
}
impl DerefMut for ListenerObservers {
    fn deref_mut(&mut self) -> &mut Observers {
        &mut self
            .owned
            .as_mut()
            .expect("live observer ownership lease")
            .tasks
    }
}
impl ObserverParking {
    pub(super) fn enqueue_system_tun(
        &self,
        owner: Arc<InboundOwner>,
        exit: Option<SystemTunProjection>,
    ) -> Result<(), EngineError> {
        let mut pending = self.system_tun.lock().unwrap();
        if pending
            .iter()
            .any(|entry| Arc::ptr_eq(&entry.owner, &owner))
        {
            return Err(std::io::Error::other(
                "original API Source observer already awaits transfer",
            )
            .into());
        }
        pending.push(SystemTunObserver { owner, exit });
        self.needs_retirement.store(true, Ordering::Release);
        drop(pending);
        self.changed.notify_one();
        Ok(())
    }
}
impl ListenerObservers {
    fn transfer_system_tun(&mut self) {
        let pending = std::mem::take(&mut *self.parking.system_tun.lock().unwrap());
        for SystemTunObserver { owner, exit } in pending {
            self.owned
                .as_mut()
                .expect("live lease")
                .tasks
                .spawn(async move {
                    #[cfg(feature = "raw-ip-runtime")]
                    let result = owner.observe_system_tun(exit).await;
                    #[cfg(not(feature = "raw-ip-runtime"))]
                    let result = {
                        let _ = exit;
                        owner.observe_listener().await
                    };
                    ObserverResult::SystemTun(result)
                });
        }
    }
    pub(crate) fn is_empty(&self) -> bool {
        self.owned.as_ref().expect("live lease").tasks.is_empty()
            && self.parking.system_tun.lock().unwrap().is_empty()
    }
    /// Mark before this generation's preparation or acquisition can suspend.
    /// An empty observer set never proves that no native owner was installed.
    pub(crate) fn begin_generation(&self) {
        self.parking.needs_retirement.store(true, Ordering::Release);
    }
    pub(crate) fn has_previous_generation(&self) -> bool {
        self.recovered
    }
    pub(crate) fn retain_failure(&mut self, error: EngineError) {
        self.owned
            .as_mut()
            .expect("live lease")
            .failure
            .get_or_insert(error);
    }
    pub(crate) fn take_failure(&mut self) -> Option<EngineError> {
        self.owned.as_mut().expect("live lease").failure.take()
    }
    fn previous_generation_retired(&mut self) {
        assert!(self.is_empty(), "original observer handles must be joined");
        assert!(
            self.owned.as_ref().expect("live lease").failure.is_none(),
            "original fault must be delivered"
        );
        self.parking
            .needs_retirement
            .store(false, Ordering::Release);
        self.recovered = false;
    }
}
impl Drop for ListenerObservers {
    fn drop(&mut self) {
        let original = self.owned.take();
        let mut parked = self
            .parking
            .parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // Only this non-clone lease could have removed the original set. Move
        // handles only: no protocol, task or external waker is dropped here.
        assert!(parked.is_none(), "exclusive original observer ownership");
        *parked = original;
    }
}
impl InboundOwnerRegistry {
    pub(super) fn mark_listener_generation(&self) {
        self.observers
            .needs_retirement
            .store(true, Ordering::Release);
    }
    /// Postcondition of successful shutdown_all_reported, not a native close
    /// receipt. Keep the bit set unless that actual cleanup succeeded first.
    pub(crate) fn confirm_observers_retired(
        &self,
        observers: &mut ListenerObservers,
    ) -> Result<(), EngineError> {
        if !Arc::ptr_eq(&self.observers, &observers.parking) {
            return Err(std::io::Error::other("foreign original observer lease").into());
        }
        let active = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let reserved = self
            .reservations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !active.is_empty()
            || !reserved.entries.is_empty()
            || !observers.is_empty()
            || observers
                .owned
                .as_ref()
                .expect("live lease")
                .failure
                .is_some()
        {
            return Err(std::io::Error::other(
                "original listener generation retirement remains unconfirmed",
            )
            .into());
        }
        // Serialize with actual owner insertion. Those paths mark dirty while
        // holding their same map lock; a late owner can never lose this bit.
        observers.previous_generation_retired();
        Ok(())
    }
    /// Inline notification only. Existing owners retain all original task and
    /// native evidence; this is never another Scope exit consumer or task.
    pub(crate) async fn observe_listener_failure(&self) -> EngineError {
        use futures_util::StreamExt;
        let owners: Vec<_> = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        let mut waiting = futures_util::stream::FuturesUnordered::new();
        for owner in owners {
            waiting.push(async move { owner.wait_unreported_failure().await });
        }
        match waiting.next().await {
            Some(error) => error,
            None => std::future::pending().await,
        }
    }

    pub(crate) fn take_listener_observers(&self) -> Result<ListenerObservers, EngineError> {
        let original = self
            .observers
            .parked
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    "inbound observer ownership is already borrowed",
                )
            })?;
        Ok(ListenerObservers {
            recovered: self.observers.needs_retirement.load(Ordering::Acquire)
                || !original.tasks.is_empty()
                || original.failure.is_some(),
            parking: self.observers.clone(),
            owned: Some(original),
        })
    }
}

#[cfg(test)]
mod tests;
