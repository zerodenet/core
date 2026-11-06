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
        assert!(
            self.owner
                .state
                .set(tokio::sync::Mutex::new(State {
                    task: Some(task),
                    result: None
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
