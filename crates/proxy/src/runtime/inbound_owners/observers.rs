//! A single persistent parking place for the original listener observer tasks.
use super::*;
use std::{
    ops::{Deref, DerefMut},
    sync::atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinSet;
mod wake;
use wake::{ObserverWake, WaitTarget};

type Observers = JoinSet<Result<(), EngineError>>;

/// The registry owns exactly one JoinSet. An orchestration may borrow its
/// ownership, but cancellation returns it instead of aborting its actual tasks.
pub(super) struct ObserverParking {
    parked: Mutex<Option<ObserverBatch>>,
    needs_retirement: AtomicBool,
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
impl ListenerObservers {
    pub(crate) async fn join_next(
        &mut self,
    ) -> Option<Result<Result<(), EngineError>, tokio::task::JoinError>> {
        self.join_next_with_id()
            .await
            .map(|result| result.map(|(_, value)| value))
    }
    pub(crate) async fn join_next_with_id(
        &mut self,
    ) -> Option<Result<(tokio::task::Id, Result<(), EngineError>), tokio::task::JoinError>> {
        let target = Arc::new(WaitTarget::default());
        let batch = self.owned.as_mut().expect("live observer ownership lease");
        let relay = batch.wake.clone();
        std::future::poll_fn(|cx| {
            // Only this borrowed join future owns the actual caller Waker.
            // The persistent original JoinSet sees a weak-target relay only.
            target.register(cx);
            relay.bind(&target);
            let deferred = relay.enter();
            let waker = std::task::Waker::from(relay.clone());
            let mut inner = std::task::Context::from_waker(&waker);
            let result = batch.tasks.poll_join_next_with_id(&mut inner);
            // Tokio can budget-yield or replace/drop its stored Waker while
            // polling. Deliver external callbacks only after that call returns.
            drop(deferred);
            result
        })
        .await
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
