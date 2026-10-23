use super::*;
#[derive(Default)]
pub(super) struct Events {
    pub(super) clones: AtomicUsize,
    pub(super) wakes: AtomicUsize,
    pub(super) last_drop: AtomicUsize,
    pub(super) receipt: Mutex<Option<DatagramCloseReceipt>>,
    pub(super) observed_live: AtomicBool,
    pub(super) owner_gone_drops: AtomicUsize,
}
#[derive(Clone, Copy)]
pub(super) enum Action {
    Check,
    CloseOnWake,
    CloseOnLastDrop,
    RevokeOnWake,
}
pub(super) struct Probe {
    entry: Weak<Entry>,
    shared: Weak<Shared>,
    control: Weak<ControlledDatagramListenerControl>,
    action: Action,
    close_once: AtomicBool,
    events: Arc<Events>,
}
impl Probe {
    fn gates(&self) -> Option<Arc<Entry>> {
        let entry = self.entry.upgrade()?;
        if let Some(shared) = self.shared.upgrade() {
            drop(
                shared
                    .retired
                    .try_lock()
                    .expect("caller RawWaker entered under retirement gate"),
            );
        }
        entry.wake_relay.assert_unlocked();
        let native = entry
            .native
            .try_lock()
            .expect("caller RawWaker entered under native gate");
        if let NativeHandleState::Live(socket) = &*native {
            assert!(socket.local_addr().unwrap().ip().is_loopback());
            self.events.observed_live.store(true, Ordering::SeqCst);
        }
        drop(native);
        Some(entry)
    }
    fn cloned(&self) {
        self.gates()
            .expect("live poll clone has its persistent original owner");
        self.events.clones.fetch_add(1, Ordering::SeqCst);
    }
    fn wake(&self) {
        let entry = self
            .gates()
            .expect("live poll wake has its persistent original owner");
        self.events.wakes.fetch_add(1, Ordering::SeqCst);
        if matches!(self.action, Action::RevokeOnWake) {
            self.control.upgrade().unwrap().revoke();
        }
        if matches!(self.action, Action::CloseOnWake) {
            self.close(&entry);
        }
    }
    fn dropped(&self, last: bool) {
        let Some(entry) = self.gates() else {
            // An earlier assertion may unwind the actual owner before its last
            // registered waker. Report that lifetime loss, but never panic a
            // second time during destruction or fabricate any close receipt.
            self.events.owner_gone_drops.fetch_add(1, Ordering::SeqCst);
            return;
        };
        if last {
            self.events.last_drop.fetch_add(1, Ordering::SeqCst);
            if matches!(self.action, Action::CloseOnLastDrop) {
                self.close(&entry);
            }
        }
    }
    fn close(&self, entry: &Entry) {
        if !self.close_once.swap(true, Ordering::SeqCst) {
            let receipt = entry
                .close()
                .expect("original native syscall confirms reentrant close");
            assert_eq!(receipt.reservation(), entry.id);
            *self.events.receipt.lock().unwrap() = Some(receipt);
        }
    }
}
unsafe fn clone(data: *const ()) -> RawWaker {
    // SAFETY: each vtable pointer owns one Arc; ManuallyDrop borrows that Arc.
    let borrowed = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Probe>()) });
    let new = Arc::clone(&borrowed);
    new.cloned();
    RawWaker::new(Arc::into_raw(new).cast(), &VTABLE)
}
unsafe fn wake(data: *const ()) {
    // SAFETY: wake consumes exactly this raw waker's Arc.
    let value = unsafe { Arc::from_raw(data.cast::<Probe>()) };
    value.wake();
}
unsafe fn wake_by_ref(data: *const ()) {
    // SAFETY: wake_by_ref borrows but does not consume the raw waker's Arc.
    let value = ManuallyDrop::new(unsafe { Arc::from_raw(data.cast::<Probe>()) });
    value.wake();
}
unsafe fn drop_waker(data: *const ()) {
    // SAFETY: drop consumes exactly this raw waker's Arc.
    let value = unsafe { Arc::from_raw(data.cast::<Probe>()) };
    value.dropped(Arc::strong_count(&value) == 1);
}
static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop_waker);
pub(super) fn watch(
    tx: &ControlledDatagramListenerTx,
    control: Weak<ControlledDatagramListenerControl>,
    action: Action,
) -> (Waker, Arc<Events>, Weak<Probe>) {
    let events = Arc::new(Events::default());
    let probe = Arc::new(Probe {
        entry: Arc::downgrade(&tx.shared.socket.entry),
        shared: Arc::downgrade(&tx.shared),
        control,
        action,
        close_once: AtomicBool::new(false),
        events: events.clone(),
    });
    let weak = Arc::downgrade(&probe);
    let raw = RawWaker::new(Arc::into_raw(probe).cast(), &VTABLE);
    // SAFETY: the raw pointer/vtable above follow Arc ownership for all methods.
    (unsafe { Waker::from_raw(raw) }, events, weak)
}
