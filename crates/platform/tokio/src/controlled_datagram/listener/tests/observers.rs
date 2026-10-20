use super::*;
use std::sync::Weak;
#[derive(Clone, Copy, Debug)]
enum On {
    Sent,
    Received,
    Error,
    Never,
    BoundaryPanic,
}
#[derive(Debug, Default)]
struct Counts {
    boundary: AtomicUsize,
    sent: AtomicUsize,
    received: AtomicUsize,
    errors: AtomicUsize,
    dropped: AtomicUsize,
}
struct ReentrantObserver {
    tx: ControlledDatagramListenerTx,
    control: Mutex<Weak<ControlledDatagramListenerControl>>,
    on: On,
    counts: Arc<Counts>,
}
impl std::fmt::Debug for ReentrantObserver {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReentrantObserver").finish_non_exhaustive()
    }
}
impl ReentrantObserver {
    fn gates(&self) {
        drop(
            self.tx
                .shared
                .retired
                .try_lock()
                .expect("shared gate held across observer callback/Drop"),
        );
        drop(
            self.tx
                .shared
                .socket
                .entry
                .native
                .try_lock()
                .expect("native gate held across observer callback/Drop"),
        );
    }
    fn retire(&self) {
        self.gates();
        let control = self.control.lock().unwrap().upgrade();
        if let Some(control) = control {
            control.revoke();
        }
    }
}
impl Drop for ReentrantObserver {
    fn drop(&mut self) {
        self.gates();
        self.counts.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
impl zero_traits::IoObserver for ReentrantObserver {
    fn datagram_boundary(&self) {
        self.gates();
        self.counts.boundary.fetch_add(1, Ordering::SeqCst);
        if matches!(self.on, On::BoundaryPanic) {
            panic!("actual boundary callback panic");
        }
    }
    fn sent(&self, _: usize) {
        self.gates();
        self.counts.sent.fetch_add(1, Ordering::SeqCst);
        if matches!(self.on, On::Sent) {
            self.retire();
        }
    }
    fn received(&self, _: usize) {
        self.gates();
        self.counts.received.fetch_add(1, Ordering::SeqCst);
        if matches!(self.on, On::Received) {
            self.retire();
        }
    }
    fn error(&self) {
        self.gates();
        self.counts.errors.fetch_add(1, Ordering::SeqCst);
        if matches!(self.on, On::Error) {
            self.retire();
        }
    }
    fn dropped(&self) {
        unreachable!()
    }
}
type Fixture = (
    ControlledDatagramOwner,
    ControlledDatagramListenerRx,
    ControlledDatagramListenerTx,
    Arc<ControlledDatagramListenerControl>,
    Arc<ReentrantObserver>,
);
fn fixture(on: On) -> Fixture {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (rx, tx, mut native) = owner.bind_listener(address(), bound(1)).unwrap();
    let observer = Arc::new(ReentrantObserver {
        tx: tx.clone(),
        control: Mutex::new(Weak::new()),
        on,
        counts: Arc::default(),
    });
    native.set_observer(Some(observer.clone())).unwrap();
    let native = Arc::new(native);
    *observer.control.lock().unwrap() = Arc::downgrade(&native);
    (owner, rx, tx, native, observer)
}
fn close(
    mut owner: ControlledDatagramOwner,
    native: Arc<ControlledDatagramListenerControl>,
    observer: Arc<ReentrantObserver>,
) {
    let counts = observer.counts.clone();
    assert_eq!(counts.boundary.load(Ordering::SeqCst), 1);
    drop(observer); // The actual original still holds its installed observer.
    let mut native = Arc::try_unwrap(native).ok().unwrap();
    native.close().unwrap();
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(owner.occupied(), 0);
}
#[tokio::test]
async fn actual_send_observer_retires_same_control_after_native_and_shared_gates_release() {
    let (mut owner, _rx, tx, native, observer) = fixture(On::Sent);
    let peer = tokio::net::UdpSocket::bind(address()).await.unwrap();
    assert_eq!(
        tx.send_to_addr(b"", peer.local_addr().unwrap())
            .await
            .unwrap(),
        0
    );
    assert_eq!(observer.counts.sent.load(Ordering::SeqCst), 1);
    assert_eq!(peer.recv_from(&mut [0; 1]).await.unwrap().0, 0);
    assert_eq!(owner.take_report().confirmed_closed, 0);
    assert_eq!(
        tx.send_to_addr(b"retired", peer.local_addr().unwrap())
            .await
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::NotConnected
    );
    close(owner, native, observer);
}
#[tokio::test]
async fn actual_receive_observer_retires_same_control_without_reclassifying_received_packet() {
    let (owner, mut rx, tx, native, observer) = fixture(On::Received);
    let peer = tokio::net::UdpSocket::bind(address()).await.unwrap();
    peer.send_to(b"received", tx.local_addr().unwrap())
        .await
        .unwrap();
    let mut bytes = [0; 16];
    assert_eq!(
        rx.recv_from_addr(&mut bytes).await.unwrap(),
        (8, peer.local_addr().unwrap())
    );
    assert_eq!(&bytes[..8], b"received");
    assert_eq!(observer.counts.received.load(Ordering::SeqCst), 1);
    close(owner, native, observer);
}
#[tokio::test]
async fn packet_send_observer_retires_same_control_without_duplicate_observation() {
    let (owner, rx, tx, native, observer) = fixture(On::Sent);
    let peer = tokio::net::UdpSocket::bind(address()).await.unwrap();
    let (_, mut sender) = crate::PacketSocketReceiver::from_granted(
        crate::contract::component::GrantedDatagramListener { rx, tx },
        crate::EgressInterfaceControl::default(),
    );
    assert_eq!(
        sender
            .send_to(b"packet", peer.local_addr().unwrap())
            .await
            .unwrap(),
        6
    );
    assert_eq!(observer.counts.sent.load(Ordering::SeqCst), 1);
    assert_eq!(peer.recv_from(&mut [0; 8]).await.unwrap().0, 6);
    close(owner, native, observer);
}
#[tokio::test]
async fn actual_native_error_observer_reentry_preserves_original_os_source() {
    let (owner, _rx, tx, native, observer) = fixture(On::Error);
    let error = tx
        .send_to_addr(&vec![0; 65536], tx.local_addr().unwrap())
        .await
        .unwrap_err();
    assert!(error.raw_os_error().is_some());
    assert_eq!(observer.counts.sent.load(Ordering::SeqCst), 0);
    assert_eq!(observer.counts.errors.load(Ordering::SeqCst), 1);
    close(owner, native, observer);
}
#[tokio::test]
async fn replacing_observer_drops_original_outside_both_native_and_listener_gates() {
    let (mut owner, _rx, _tx, native, observer) = fixture(On::Never);
    let counts = observer.counts.clone();
    drop(observer);
    let mut native = Arc::try_unwrap(native).ok().unwrap();
    native.set_observer(None).unwrap();
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}

#[tokio::test]
async fn boundary_callback_panic_keeps_installed_observer_and_original_in_persistent_owner() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (_rx, tx, mut native) = owner.bind_listener(address(), bound(1)).unwrap();
    let observer = Arc::new(ReentrantObserver {
        tx,
        control: Mutex::new(Weak::new()),
        on: On::BoundaryPanic,
        counts: Arc::default(),
    });
    let counts = observer.counts.clone();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        native.set_observer(Some(observer.clone())).unwrap();
    }));
    assert!(result.is_err());
    assert_eq!(owner.occupied(), 1);
    assert_eq!(owner.take_report().confirmed_closed, 0);
    drop(observer);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 0);
    native.close().unwrap();
    assert_eq!(counts.boundary.load(Ordering::SeqCst), 1);
    assert_eq!(counts.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
