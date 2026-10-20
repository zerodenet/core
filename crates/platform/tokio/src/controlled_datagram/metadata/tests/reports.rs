use super::*;
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::atomic::AtomicBool,
};
struct PanicWake(AtomicBool);
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        if self.0.swap(false, Ordering::SeqCst) {
            panic!("actual probe receive wake");
        }
    }
}
#[tokio::test]
async fn actual_probe_wake_panic_preserves_preceding_main_native_receipt_for_retry() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let grant = owner.bind(address()).unwrap();
    // A real capability-probe original in the existing probe owner. Register a
    // real pending receive to exercise arbitrary wake code during retirement.
    let raw = owner.probes.owner.bind_raw_listener(address()).unwrap();
    let probe = owner.probes.owner.register_native(raw).unwrap();
    let waker = Waker::from(Arc::new(PanicWake(AtomicBool::new(true))));
    let mut bytes = [0; 1];
    assert!(probe
        .poll_recv_from_addr(&mut Context::from_waker(&waker), &mut bytes)
        .is_pending());
    assert!(catch_unwind(AssertUnwindSafe(|| owner.close_all())).is_err());
    assert_eq!(
        grant.tx.local_addr().unwrap_err().kind(),
        std_io::ErrorKind::NotConnected
    );
    assert!(
        probe.local_addr().is_ok(),
        "probe wake panic happened before its original native close"
    );
    assert_eq!(owner.pending_main.as_ref().unwrap().confirmed_closed, 1);
    let denied = owner.bind(address()).err().unwrap();
    assert!(matches!(denied.outcome, AcquisitionOutcome::KnownNoNative));
    assert!(denied.reservation.is_none());
    let pending = owner.take_report();
    assert_eq!(pending.main.confirmed_closed, 1);
    assert_eq!(pending.probes.retained_live, 1);
    let closed = owner.close_all();
    assert_eq!(
        closed.main.confirmed_closed, 0,
        "original main receipt is delivered once"
    );
    assert_eq!(closed.probes.confirmed_closed, 1);
    assert!(closed.is_complete());
    assert_eq!(owner.take_report().main.confirmed_closed, 0);
}
