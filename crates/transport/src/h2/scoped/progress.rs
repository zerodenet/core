//! One original connection's synchronous poll boundary and deferred wakes.
use super::*;
use futures_util::task::{ArcWake, AtomicWaker};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Mutex, MutexGuard,
};
pub(super) struct Deferred {
    gate: Arc<AtomicUsize>,
    pending: AtomicBool,
    actual: AtomicWaker,
}
impl Deferred {
    fn new(gate: Arc<AtomicUsize>) -> Arc<Self> {
        Arc::new(Self {
            gate,
            pending: AtomicBool::new(false),
            actual: AtomicWaker::new(),
        })
    }
    pub(super) fn register(&self, wake: &std::task::Waker) {
        self.actual.register(wake);
    }
    fn release(&self) {
        if self.pending.swap(false, Ordering::AcqRel) {
            self.actual.wake();
        }
    }
}
impl ArcWake for Deferred {
    fn wake_by_ref(this: &Arc<Self>) {
        this.pending.store(true, Ordering::Release);
        if this.gate.load(Ordering::Acquire) == 0 {
            this.release();
        }
    }
}
pub(super) struct Progress {
    gate: Mutex<()>,
    entered: Arc<AtomicUsize>,
    pub(super) driver: Arc<Deferred>,
    pub(super) writer: Arc<Deferred>,
    pub(super) reader: Arc<Deferred>,
    pub(super) flushed: Arc<super::super::owned::Flushed>,
    pub(super) failure: Mutex<Option<Arc<io::Error>>>,
}
impl Default for Progress {
    fn default() -> Self {
        let entered = Arc::new(AtomicUsize::new(0));
        Self {
            driver: Deferred::new(entered.clone()),
            writer: Deferred::new(entered.clone()),
            reader: Deferred::new(entered.clone()),
            gate: Mutex::new(()),
            entered,
            flushed: Arc::default(),
            failure: Mutex::new(None),
        }
    }
}
impl Progress {
    pub(super) fn lock(&self) -> Gate<'_> {
        // Count known contenders too. The guarantee is that this poll releases
        // its own mutex before invoking external wakers; another thread may
        // legitimately begin its own serialized poll before that callback.
        self.entered.fetch_add(1, Ordering::AcqRel);
        Gate {
            progress: self,
            guard: Some(self.gate.lock().unwrap()),
        }
    }
}
pub(super) struct Gate<'a> {
    progress: &'a Progress,
    guard: Option<MutexGuard<'a, ()>>,
}
impl Drop for Gate<'_> {
    fn drop(&mut self) {
        drop(self.guard.take());
        if self.progress.entered.fetch_sub(1, Ordering::AcqRel) == 1 {
            // Neither application nor connection wakers run inside this poll
            // gate, including h2's synchronous send_data/credit notifications.
            self.progress.driver.release();
            self.progress.writer.release();
            self.progress.reader.release();
        }
    }
}
pub(super) struct Ended(pub(super) Arc<Progress>);
impl Drop for Ended {
    fn drop(&mut self) {
        self.0.flushed.interrupt();
        futures_util::task::ArcWake::wake_by_ref(&self.0.writer);
        futures_util::task::ArcWake::wake_by_ref(&self.0.reader);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Reentrant {
        owner: std::sync::Weak<Progress>,
        wakes: AtomicUsize,
    }
    impl ArcWake for Reentrant {
        fn wake_by_ref(this: &Arc<Self>) {
            let owner = this.owner.upgrade().unwrap();
            // An executor is permitted to synchronously inspect its owner.
            // This external callback must never run from inside the IO gate.
            assert!(owner.gate.try_lock().is_ok());
            this.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn original_reader_writer_and_connection_wakes_are_external_to_the_same_poll_gate() {
        let progress = Arc::new(Progress::default());
        let actual = Arc::new(Reentrant {
            owner: Arc::downgrade(&progress),
            wakes: AtomicUsize::new(0),
        });
        let wake = futures_util::task::waker_ref(&actual);
        progress.driver.register(&wake);
        progress.writer.register(&wake);
        progress.reader.register(&wake);
        let gate = progress.lock();
        ArcWake::wake_by_ref(&progress.driver);
        ArcWake::wake_by_ref(&progress.writer);
        ArcWake::wake_by_ref(&progress.reader);
        assert_eq!(actual.wakes.load(Ordering::SeqCst), 0);
        drop(gate);
        assert_eq!(actual.wakes.load(Ordering::SeqCst), 3);
    }
}
