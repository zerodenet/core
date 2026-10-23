use super::*;
#[derive(Debug)]
struct CloseDuringNativeDetach {
    entry: Weak<Entry>,
    seen: Arc<Mutex<Option<Arc<std_io::Error>>>>,
}
impl zero_traits::IoObserver for CloseDuringNativeDetach {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
impl Drop for CloseDuringNativeDetach {
    fn drop(&mut self) {
        let entry = self.entry.upgrade().unwrap();
        entry.wake_relay.assert_unlocked();
        let native = entry
            .native
            .try_lock()
            .expect("native teardown must not hold state gate");
        let NativeHandleState::Unknown(source) = &*native else {
            panic!("in-flight consuming native close cannot publish Closed")
        };
        let expected = source.clone();
        drop(native);
        let nested = entry
            .close()
            .expect_err("native teardown has no committed close receipt yet");
        assert!(Arc::ptr_eq(&expected, &nested));
        *self.seen.lock().unwrap() = Some(nested);
    }
}
#[tokio::test]
async fn reentrant_native_teardown_preserves_unknown_until_actual_original_receipt_is_committed() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (_rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
    let seen = Arc::new(Mutex::new(None));
    control
        .set_observer(Some(Arc::new(CloseDuringNativeDetach {
            entry: Arc::downgrade(&tx.shared.socket.entry),
            seen: seen.clone(),
        })))
        .unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 0);
    assert_eq!(owner.occupied(), 1);
    let receipt = control.close().unwrap();
    assert!(seen.lock().unwrap().is_some());
    assert_eq!(receipt.reservation(), control.reservation());
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.occupied(), 0);
    assert_eq!(control.close().unwrap(), receipt);
    assert_eq!(owner.take_report().confirmed_closed, 0);
}
