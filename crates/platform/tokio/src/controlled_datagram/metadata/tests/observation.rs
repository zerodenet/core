use super::*;
use std::{fmt, sync::Weak};
use zero_traits::IoObserver;

#[derive(Debug, Default)]
struct Counts {
    boundaries: AtomicUsize,
    sent: AtomicUsize,
    sent_packets: AtomicUsize,
    received: AtomicUsize,
    received_packets: AtomicUsize,
    errors: AtomicUsize,
}
impl IoObserver for Counts {
    fn datagram_boundary(&self) {
        self.boundaries.fetch_add(1, Ordering::SeqCst);
    }
    fn received(&self, bytes: usize) {
        self.received.fetch_add(bytes, Ordering::SeqCst);
        self.received_packets.fetch_add(1, Ordering::SeqCst);
    }
    fn sent(&self, bytes: usize) {
        self.sent.fetch_add(bytes, Ordering::SeqCst);
        self.sent_packets.fetch_add(1, Ordering::SeqCst);
    }
    fn error(&self) {
        self.errors.fetch_add(1, Ordering::SeqCst);
    }
    fn dropped(&self) {}
}
async fn receive(rx: &mut MetadataDatagramRx) -> Vec<u8> {
    let mut payload = [0; 64];
    let mut buffers = [IoSliceMut::new(&mut payload)];
    let mut metadata = [MetadataReceive::default()];
    assert_eq!(
        poll_fn(|cx| rx.poll_recv(cx, &mut buffers, &mut metadata))
            .await
            .unwrap(),
        1
    );
    buffers[0][..metadata[0].len].to_vec()
}
#[tokio::test]
async fn actual_zero_and_payload_count_native_datagrams_and_oversize_keeps_os_source() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        mut control,
    } = owner.bind(address()).unwrap();
    let counts = Arc::new(Counts::default());
    control.set_observer(Some(counts.clone())).unwrap();
    let local = tx.local_addr().unwrap();
    for bytes in [&b""[..], &b"native"[..]] {
        assert_eq!(send(&tx, &transmit(local, bytes)).await.bytes, bytes.len());
        assert_eq!(receive(&mut rx).await, bytes);
    }
    let mut poller = tx.create_write_poller().unwrap();
    poll_fn(|cx| poller.poll_writable(cx)).await.unwrap();
    let oversize = vec![0; 65536];
    let error = tx.try_send(&transmit(local, &oversize)).unwrap_err();
    assert!(error.raw_os_error().is_some());
    assert_eq!(counts.boundaries.load(Ordering::SeqCst), 1);
    assert_eq!(counts.sent.load(Ordering::SeqCst), 6);
    assert_eq!(counts.received.load(Ordering::SeqCst), 6);
    assert_eq!(counts.sent_packets.load(Ordering::SeqCst), 2);
    assert_eq!(counts.received_packets.load(Ordering::SeqCst), 2);
    assert_eq!(counts.errors.load(Ordering::SeqCst), 1);
    control.close().unwrap();
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
async fn actual_gso_and_gro_observation_counts_segments_and_partial_final_payload() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        mut control,
    } = owner.bind(address()).unwrap();
    let counts = Arc::new(Counts::default());
    control.set_observer(Some(counts.clone())).unwrap();
    if tx.capabilities().transmit_segments < 3 {
        control.close().unwrap();
        return;
    }
    let mut message = transmit(tx.local_addr().unwrap(), b"onetwox");
    message.segment_size = Some(3);
    assert_eq!(send(&tx, &message).await.datagrams, 3);
    let mut payload = Vec::new();
    while payload.len() < 7 {
        payload.extend_from_slice(&receive(&mut rx).await);
    }
    assert_eq!(payload, b"onetwox");
    assert_eq!(counts.sent.load(Ordering::SeqCst), 7);
    assert_eq!(counts.received.load(Ordering::SeqCst), 7);
    assert_eq!(counts.sent_packets.load(Ordering::SeqCst), 3);
    assert_eq!(counts.received_packets.load(Ordering::SeqCst), 3);
    control.close().unwrap();
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}

struct Reenter {
    shared: Weak<Shared>,
    control: Weak<Mutex<MetadataDatagramControl>>,
    receive: bool,
    panic: bool,
    calls: AtomicUsize,
}
impl fmt::Debug for Reenter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reenter").finish_non_exhaustive()
    }
}
impl Reenter {
    fn check(&self) {
        let shared = self.shared.upgrade().unwrap();
        assert!(shared.socket.entry.native.try_lock().is_ok());
    }
    fn complete(&self) {
        self.check();
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.panic {
            panic!("actual metadata observer callback");
        }
        self.control
            .upgrade()
            .unwrap()
            .lock()
            .unwrap()
            .close()
            .unwrap();
    }
}
impl IoObserver for Reenter {
    fn datagram_boundary(&self) {
        self.check();
    }
    fn received(&self, _bytes: usize) {
        if self.receive {
            self.complete();
        }
    }
    fn sent(&self, _bytes: usize) {
        if !self.receive {
            self.complete();
        }
    }
    fn error(&self) {
        self.check();
    }
    fn dropped(&self) {}
}
#[tokio::test]
async fn actual_send_observer_can_synchronously_close_original_without_early_receipt() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        rx: _rx,
        tx,
        control,
    } = owner.bind(address()).unwrap();
    let observer = reentrant(&tx, control, false, false);
    let local = tx.local_addr().unwrap();
    assert_eq!(send(&tx, &transmit(local, b"sent")).await.bytes, 4);
    assert_eq!(observer.1.calls.load(Ordering::SeqCst), 1);
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
    assert_eq!(owner.occupied_main(), 0);
    assert_eq!(
        tx.local_addr().unwrap_err().kind(),
        std_io::ErrorKind::NotConnected
    );
}
#[tokio::test]
async fn actual_receive_observer_can_synchronously_close_original_after_payload_received() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        control,
    } = owner.bind(address()).unwrap();
    let local = tx.local_addr().unwrap();
    send(&tx, &transmit(local, b"received")).await;
    let observer = reentrant(&tx, control, true, false);
    assert_eq!(receive(&mut rx).await, b"received");
    assert_eq!(observer.1.calls.load(Ordering::SeqCst), 1);
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
#[tokio::test]
async fn actual_submission_observer_panic_retains_original_until_explicit_native_close() {
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let GrantedMetadataDatagram {
        mut rx,
        tx,
        control,
    } = owner.bind(address()).unwrap();
    let local = tx.local_addr().unwrap();
    let observer = reentrant(&tx, control, false, true);
    let mut poller = tx.create_write_poller().unwrap();
    poll_fn(|cx| poller.poll_writable(cx)).await.unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tx.try_send(&transmit(local, b"already submitted"))
    }));
    assert!(result.is_err());
    assert_eq!(observer.1.calls.load(Ordering::SeqCst), 1);
    assert_eq!(owner.occupied_main(), 1);
    assert_eq!(owner.take_report().main.confirmed_closed, 0);
    observer.0.lock().unwrap().set_observer(None).unwrap();
    assert_eq!(receive(&mut rx).await, b"already submitted");
    observer.0.lock().unwrap().close().unwrap();
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
fn reentrant(
    tx: &MetadataDatagramTx,
    control: MetadataDatagramControl,
    receive: bool,
    panic: bool,
) -> (Arc<Mutex<MetadataDatagramControl>>, Arc<Reenter>) {
    let control = Arc::new(Mutex::new(control));
    let observer = Arc::new(Reenter {
        shared: Arc::downgrade(&tx.shared),
        control: Arc::downgrade(&control),
        receive,
        panic,
        calls: AtomicUsize::new(0),
    });
    control
        .lock()
        .unwrap()
        .set_observer(Some(observer.clone()))
        .unwrap();
    (control, observer)
}
#[test]
fn accounting_partial_prefix_does_not_count_unsubmitted_suffix() {
    // Arithmetic over an actual outcome shape; this is not an Apple runtime test.
    let observer = Arc::new(Counts::default());
    super::super::observation::send(
        Some(observer.clone()),
        &Ok(MetadataSendOutcome {
            bytes: 5,
            datagrams: 2,
            complete: false,
        }),
        Some(3),
    );
    assert_eq!(observer.sent.load(Ordering::SeqCst), 5);
    assert_eq!(observer.sent_packets.load(Ordering::SeqCst), 2);
}
