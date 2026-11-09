//! The existing one-task owner reserves its actual slot before startup IO/spawn.
use std::{
    io,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, OnceLock,
    },
};
const COLD: u8 = 0;
const PREPARING: u8 = 1;
const INSTALLED: u8 = 2;
const EMPTY: u8 = 3;
pub(crate) struct ControlTask {
    phase: AtomicU8,
    state: OnceLock<tokio::sync::Mutex<State>>,
    changed: tokio::sync::Notify,
}
struct State {
    task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    result: Option<Result<(), Arc<io::Error>>>,
    delivered: Option<Arc<DeliveredFailure>>,
    native: tokio::task::Id,
}
// One bounded diagnostic cell for this exact original execution. Its Weak
// preserves source identity while allowing the accepted payload to be freed.
struct DeliveredFailure {
    source: std::sync::Weak<io::Error>,
    kind: io::ErrorKind,
    native: tokio::task::Id,
}
#[derive(Clone)]
struct FailureTransferred(Arc<DeliveredFailure>);
impl std::fmt::Debug for FailureTransferred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlTaskFailureTransferred")
            .field("native_task", &self.0.native)
            .field("kind", &self.0.kind)
            .finish()
    }
}
impl std::fmt::Display for FailureTransferred {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "original control task {} failed; its error payload was already handed to its receiver",
            self.0.native
        )
    }
}
impl std::error::Error for FailureTransferred {}
impl DeliveredFailure {
    fn observed(self: &Arc<Self>) -> io::Error {
        match self.source.upgrade() {
            Some(actual) => io::Error::new(self.kind, actual),
            None => io::Error::new(self.kind, FailureTransferred(self.clone())),
        }
    }
}
pub(crate) struct ControlTaskStart<'a> {
    owner: &'a ControlTask,
    installed: bool,
}
impl ControlTask {
    pub(crate) fn empty() -> Self {
        Self {
            phase: AtomicU8::new(COLD),
            state: OnceLock::new(),
            changed: tokio::sync::Notify::new(),
        }
    }
    pub(crate) fn reserve(&self) -> io::Result<ControlTaskStart<'_>> {
        self.phase
            .compare_exchange(COLD, PREPARING, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| {
                io::Error::other("control original task admission already reserved or stopped")
            })?;
        Ok(ControlTaskStart {
            owner: self,
            installed: false,
        })
    }
    #[cfg(test)]
    pub(crate) fn new(task: tokio::task::JoinHandle<io::Result<()>>) -> Self {
        let owner = Self::empty();
        owner.reserve().unwrap().install(task);
        owner
    }
    #[cfg(test)]
    pub(super) async fn original_retained_for_test(&self) -> bool {
        match self.state.get() {
            Some(state) => state.lock().await.task.is_some(),
            None => false,
        }
    }
    #[cfg(test)]
    pub(super) async fn original_fault_for_test(&self) -> Option<Arc<io::Error>> {
        self.state
            .get()?
            .lock()
            .await
            .result
            .as_ref()?
            .as_ref()
            .err()
            .cloned()
    }
    /// The actual joined failure is already strongly held by this exact
    /// receiver. Retire only its local payload copy. Keep the original failure
    /// fact/identity forever in this ended slot; no IO or Scope fact changes.
    pub(crate) fn release_joined_failure(&self, received: &io::Error) -> bool {
        let Some(state) = self.state.get() else {
            return false;
        };
        let Ok(mut state) = state.try_lock() else {
            return false;
        };
        if state.task.is_some() {
            return false;
        }
        if let Some(delivered) = &state.delivered {
            return received.get_ref().is_some_and(|source| {
                source
                    .downcast_ref::<Arc<io::Error>>()
                    .is_some_and(|actual| {
                        std::sync::Weak::ptr_eq(&delivered.source, &Arc::downgrade(actual))
                    })
                    || source
                        .downcast_ref::<FailureTransferred>()
                        .is_some_and(|actual| Arc::ptr_eq(delivered, &actual.0))
            });
        }
        let Some(actual) = received
            .get_ref()
            .and_then(|source| source.downcast_ref::<Arc<io::Error>>())
        else {
            return false;
        };
        if !matches!(state.result.as_ref(),Some(Err(source)) if Arc::ptr_eq(source,actual)) {
            return false;
        }
        state.delivered = Some(Arc::new(DeliveredFailure {
            source: Arc::downgrade(actual),
            kind: actual.kind(),
            native: state.native,
        }));
        let original = state.result.take();
        drop(state);
        drop(original);
        true
    }
    pub(crate) fn close_admission(&self) {
        // Only explicit shutdown fences a cold task slot. Observation cannot
        // revoke a startup that has not yet registered its original.
        let _ = self
            .phase
            .compare_exchange(COLD, EMPTY, Ordering::AcqRel, Ordering::Acquire);
    }
    pub(crate) async fn join(&self) -> io::Result<()> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            match self.phase.load(Ordering::Acquire) {
                COLD | EMPTY => return Ok(()),
                PREPARING => changed.await,
                INSTALLED => break,
                _ => unreachable!("private original task phase"),
            }
        }
        let mut state = self
            .state
            .get()
            .expect("actual task installed before publication")
            .lock()
            .await;
        if let Some(delivered) = &state.delivered {
            return Err(delivered.observed());
        }
        if state.result.is_none() {
            let joined = state
                .task
                .as_mut()
                .expect("same retained original task")
                .await;
            state.result = Some(
                joined
                    .map_err(io::Error::other)
                    .and_then(|result| result)
                    .map_err(Arc::new),
            );
            state.task = None;
        }
        state
            .result
            .as_ref()
            .unwrap()
            .clone()
            .map_err(|source| io::Error::new(source.kind(), source))
    }
}
impl ControlTaskStart<'_> {
    // Unique reservation, no await and no fallible mutex admission after spawn.
    pub(crate) fn install(mut self, task: tokio::task::JoinHandle<io::Result<()>>) {
        let native = task.id();
        assert!(
            self.owner
                .state
                .set(tokio::sync::Mutex::new(State {
                    task: Some(task),
                    result: None,
                    delivered: None,
                    native,
                }))
                .is_ok(),
            "unique original task reservation"
        );
        self.installed = true;
        self.owner.phase.store(INSTALLED, Ordering::Release);
        self.owner.changed.notify_waiters();
    }
}
impl Drop for ControlTaskStart<'_> {
    fn drop(&mut self) {
        if !self.installed {
            // No spawn occurred. Native startup resources, if any, remain in
            // their independent concrete owner until its matching cleanup.
            self.owner.phase.store(EMPTY, Ordering::Release);
            self.owner.changed.notify_waiters();
        }
    }
}
