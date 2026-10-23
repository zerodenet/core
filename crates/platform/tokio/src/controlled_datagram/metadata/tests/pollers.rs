use super::*;
use std::sync::Weak;
struct Reenter {
    tx: MetadataDatagramTx,
    control: Weak<MetadataDatagramControl>,
    wakes: AtomicUsize,
    notified: tokio::sync::Notify,
}
impl Wake for Reenter {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        // Fail promptly if native readiness or cancellation calls a user waker
        // while holding the original gate. Then really reenter same Control.
        drop(
            self.tx
                .shared
                .socket
                .entry
                .native
                .try_lock()
                .expect("native gate held across external wake"),
        );
        self.wakes.fetch_add(1, Ordering::SeqCst);
        self.notified.notify_one();
        if let Some(control) = self.control.upgrade() {
            control.revoke();
        }
    }
}
#[tokio::test]
async fn bounded_independent_native_pollers_broadcast_revoke_with_original_retained() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        control,
    } = owner.bind(address()).unwrap();
    let control = Arc::new(control);
    let mut first = tx.create_write_poller().unwrap();
    let mut second = tx.create_write_poller().unwrap();
    assert!(
        matches!(tx.create_write_poller(), Err(error) if error.kind() == std_io::ErrorKind::WouldBlock)
    );
    let wake = Arc::new(Reenter {
        tx: tx.clone(),
        control: Arc::downgrade(&control),
        wakes: AtomicUsize::new(0),
        notified: tokio::sync::Notify::new(),
    });
    let waker = Waker::from(wake.clone());
    let mut cx = Context::from_waker(&waker);
    // Before any reactor turn these are actual native readiness suspensions.
    assert!(first.poll_writable(&mut cx).is_pending());
    assert!(second.poll_writable(&mut cx).is_pending());
    let mut bytes = [0; 1];
    let mut buffers = [IoSliceMut::new(&mut bytes)];
    let mut metadata = [MetadataReceive::default()];
    assert!(rx
        .poll_recv(&mut cx, &mut buffers, &mut metadata)
        .is_pending());
    // The synchronous callback reenters this same actual controller.
    tx.shared.socket.entry.retired.cancel();
    assert!(wake.wakes.load(Ordering::SeqCst) >= 3);
    assert_eq!(owner.take_report().main.confirmed_closed, 0);
    assert_eq!(owner.occupied_main(), 1);
    assert!(
        matches!(first.poll_writable(&mut cx), Poll::Ready(Err(error)) if error.kind() == std_io::ErrorKind::NotConnected)
    );
    assert!(
        matches!(second.poll_writable(&mut cx), Poll::Ready(Err(error)) if error.kind() == std_io::ErrorKind::NotConnected)
    );
    assert!(
        matches!(rx.poll_recv(&mut cx, &mut buffers, &mut metadata), Poll::Ready(Err(error)) if error.kind() == std_io::ErrorKind::NotConnected)
    );
    match Arc::try_unwrap(control) {
        Ok(mut control) => {
            control.close().unwrap();
        }
        Err(_) => panic!("sole native control"),
    };
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
#[tokio::test]
async fn native_writable_wake_reenters_control_only_after_original_gate_releases() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        rx: _rx,
        tx,
        control,
    } = owner.bind(address()).unwrap();
    let control = Arc::new(control);
    let wake = Arc::new(Reenter {
        tx: tx.clone(),
        control: Arc::downgrade(&control),
        wakes: AtomicUsize::new(0),
        notified: tokio::sync::Notify::new(),
    });
    let waker = Waker::from(wake.clone());
    let mut poller = tx.create_write_poller().unwrap();
    assert!(poller
        .poll_writable(&mut Context::from_waker(&waker))
        .is_pending());
    // The reactor really drives the internal writer waker; the user callback
    // revokes the original. No fabricated readiness/timeout is used as proof.
    wake.notified.notified().await;
    assert!(tx.shared.socket.entry.retired.is_cancelled());
    match Arc::try_unwrap(control) {
        Ok(mut control) => {
            control.close().unwrap();
        }
        Err(_) => panic!("sole native control"),
    };
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
#[tokio::test]
async fn dropped_poller_returns_only_its_slot_and_sender_drop_keeps_same_native_original_live() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        rx: _rx,
        tx,
        mut control,
    } = owner.bind(address()).unwrap();
    let first = tx.create_write_poller().unwrap();
    let second = tx.create_write_poller().unwrap();
    drop(first);
    let third = tx.create_write_poller().unwrap();
    assert!(tx.create_write_poller().is_err());
    drop(second);
    drop(third);
    drop(tx.clone());
    let peer = tokio::net::UdpSocket::bind(address()).await.unwrap();
    let outcome = send(&tx, &transmit(peer.local_addr().unwrap(), b"live")).await;
    assert_eq!(outcome.bytes, 4);
    assert_eq!(
        peer.recv_from(&mut [0; 8]).await.unwrap(),
        (4, tx.local_addr().unwrap())
    );
    control.close().unwrap();
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
struct PanicWake;
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("actual external wake panic");
    }
}
#[tokio::test]
async fn external_wake_panic_keeps_original_and_allows_real_owner_close_retry() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        rx: _rx,
        tx,
        control,
    } = owner.bind(address()).unwrap();
    let mut poller = tx.create_write_poller().unwrap();
    let waker = Waker::from(Arc::new(PanicWake));
    assert!(poller
        .poll_writable(&mut Context::from_waker(&waker))
        .is_pending());
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| control.revoke()));
    assert!(failed.is_err());
    assert_eq!(owner.occupied_main(), 1);
    assert_eq!(owner.take_report().main.confirmed_closed, 0);
    let report = owner.close_all();
    assert_eq!(report.main.confirmed_closed, 1);
    assert!(report.is_complete());
}
