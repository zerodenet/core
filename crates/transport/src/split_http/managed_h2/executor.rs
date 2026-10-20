use super::failure::Failure;
use std::{
    collections::VecDeque,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{mpsc, Notify};
use zero_platform_tokio::contract::{Reason, Stage};

pub(super) type Work = Pin<Box<dyn Future<Output = ()> + Send>>;
struct Admission {
    // Unit occupancy tokens remain queued until the corresponding original
    // future exits or is dropped. Moving work to the active set frees no slot.
    budget: mpsc::Sender<()>,
    occupied: mpsc::Receiver<()>,
    queued: VecDeque<Work>,
}
pub(super) struct Inbox {
    state: Mutex<Admission>,
    pub(super) changed: Notify,
    pub(super) failure: Arc<Failure>,
}
impl Inbox {
    pub(super) fn new(limit: NonZeroUsize, failure: Arc<Failure>) -> Arc<Self> {
        let (budget, occupied) = mpsc::channel(limit.get());
        Arc::new(Self {
            state: Mutex::new(Admission {
                budget,
                occupied,
                queued: VecDeque::new(),
            }),
            changed: Notify::new(),
            failure,
        })
    }
    fn state(&self) -> std::sync::MutexGuard<'_, Admission> {
        self.state
            .lock()
            .unwrap_or_else(|source| source.into_inner())
    }
    pub(super) fn pop(&self) -> Option<Work> {
        self.state().queued.pop_front()
    }
    pub(super) fn close(&self) {
        self.state().occupied.close();
        self.changed.notify_waiters();
    }
    pub(super) fn executor(self: &Arc<Self>) -> Executor {
        Executor(Arc::downgrade(self), self.failure.clone())
    }
    #[cfg(test)]
    pub(super) fn occupied(&self) -> usize {
        self.state().occupied.len()
    }
}
// Pending and active work owns only a Weak return path. E stored inside a
// Hyper delegate cannot keep its own receiver/active future/IO alive forever.
struct Occupancy(Weak<Inbox>);
impl Drop for Occupancy {
    fn drop(&mut self) {
        if let Some(inbox) = self.0.upgrade() {
            let result = inbox.state().occupied.try_recv();
            debug_assert!(result.is_ok(), "one occupancy token per original delegate");
        }
    }
}
#[derive(Clone)]
pub(super) struct Executor(Weak<Inbox>, Arc<Failure>);
impl<F> hyper::rt::Executor<F> for Executor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        let Some(inbox) = self.0.upgrade() else {
            self.1.set(self.1.local(
                Stage::Prepare,
                Reason::Revoked,
                std::io::Error::other("H2 delegate owner exited"),
            ));
            return;
        };
        let result = {
            let mut state = inbox.state();
            // This actual error contains (), never the rejected F. Its original
            // source is safe to retain after every F and original IO is released.
            let reserved = state.budget.try_reserve().map(|permit| permit.send(()));
            match reserved {
                Ok(()) => {
                    let occupancy = Occupancy(Arc::downgrade(&inbox));
                    state.queued.push_back(Box::pin(async move {
                        let _occupancy = occupancy;
                        future.await;
                    }));
                    Ok(())
                }
                Err(source) => Err(source),
            }
        };
        if let Err(source) = result {
            let reason = match source {
                mpsc::error::TrySendError::Full(()) => Reason::Capacity,
                mpsc::error::TrySendError::Closed(()) => Reason::Revoked,
            };
            inbox
                .failure
                .set(inbox.failure.local(Stage::Prepare, reason, source));
        }
        inbox.changed.notify_one();
    }
}
