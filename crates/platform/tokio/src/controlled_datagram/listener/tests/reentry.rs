use super::*;
use std::sync::Weak;

struct Reentry {
    tx: ControlledDatagramListenerTx,
    control: Option<Weak<ControlledDatagramListenerControl>>,
    calls: AtomicUsize,
}
impl Reentry {
    fn check(&self) {
        // Fail promptly on the old implementation rather than parking the
        // whole test binary in a recursive mutex acquisition.
        drop(
            self.tx
                .shared
                .retired
                .try_lock()
                .expect("retirement gate held across external wake"),
        );
        drop(
            self.tx
                .shared
                .socket
                .entry
                .native
                .try_lock()
                .expect("native gate held across external wake"),
        );
        let mut send = Box::pin(self.tx.send_to_addr(b"reentry", address()));
        let waker = Waker::noop();
        match send.as_mut().poll(&mut Context::from_waker(waker)) {
            Poll::Ready(Err(error)) => assert_eq!(error.kind(), std_io::ErrorKind::NotConnected),
            _ => panic!("a synchronous reentrant send must observe retired admission"),
        }
        if let Some(control) = self.control.as_ref().and_then(Weak::upgrade) {
            control.revoke(); // The same actual Control, with no test-only clone.
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}
impl Wake for Reentry {
    fn wake(self: Arc<Self>) {
        self.check();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.check();
    }
}
fn suspend<F: Future>(future: std::pin::Pin<&mut F>, wake: &Arc<Reentry>) {
    let waker = Waker::from(wake.clone());
    assert!(future.poll(&mut Context::from_waker(&waker)).is_pending());
}
#[tokio::test]
async fn revoke_wakes_real_pending_io_outside_gate_and_allows_same_control_reentry() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, native) = owner.bind_listener(address(), bound(1)).unwrap();
    let native = Arc::new(native);
    let wake = Arc::new(Reentry {
        tx: tx.clone(),
        control: Some(Arc::downgrade(&native)),
        calls: AtomicUsize::new(0),
    });
    let local = tx.local_addr().unwrap();
    let mut buffer = [0; 8];
    let read = rx.recv_from_addr(&mut buffer);
    tokio::pin!(read);
    suspend(read.as_mut(), &wake);
    let send = tx.send_to_addr(b"pending", local);
    tokio::pin!(send);
    suspend(send.as_mut(), &wake);
    native.revoke();
    assert!(wake.calls.load(Ordering::SeqCst) >= 2);
    let (read, send) = tokio::join!(read, send);
    assert_eq!(read.unwrap_err().kind(), std_io::ErrorKind::NotConnected);
    assert_eq!(send.unwrap_err().kind(), std_io::ErrorKind::NotConnected);
    assert_eq!(owner.occupied(), 1);
    assert_eq!(owner.take_report().confirmed_closed, 0);
    let mut native = Arc::try_unwrap(native)
        .ok()
        .expect("waker retains only Weak Control");
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.occupied(), 0);
}
#[tokio::test]
async fn close_wakes_real_pending_io_before_native_deregistration_without_gate_reentry() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut native) = owner.bind_listener(address(), bound(1)).unwrap();
    let wake = Arc::new(Reentry {
        tx: tx.clone(),
        control: None,
        calls: AtomicUsize::new(0),
    });
    let local = tx.local_addr().unwrap();
    let mut buffer = [0; 8];
    let read = rx.recv_from_addr(&mut buffer);
    tokio::pin!(read);
    suspend(read.as_mut(), &wake);
    let send = tx.send_to_addr(b"pending", local);
    tokio::pin!(send);
    suspend(send.as_mut(), &wake);
    let receipt = native.close().unwrap();
    assert_eq!(receipt.reservation(), native.reservation());
    assert!(wake.calls.load(Ordering::SeqCst) >= 2);
    let (read, send) = tokio::join!(read, send);
    assert_eq!(read.unwrap_err().kind(), std_io::ErrorKind::NotConnected);
    assert_eq!(send.unwrap_err().kind(), std_io::ErrorKind::NotConnected);
    assert_eq!(tx.shared.sends.available_permits(), 1);
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.occupied(), 0);
}
