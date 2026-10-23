use super::*;
use crate::contract::scope::TaskScope;

#[tokio::test]
async fn actual_metadata_views_are_released_before_private_receipt_but_native_remains_host_owned() {
    let scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let mut grant = owner.bind(address()).unwrap();
    let (issuer, claim) = grant
        .control
        .claim_views(scope.registrar().scope_identity())
        .unwrap();
    assert_eq!(
        grant
            .control
            .claim_views(scope.registrar().scope_identity())
            .err()
            .unwrap()
            .kind(),
        std_io::ErrorKind::AlreadyExists
    );
    let extra = grant.tx.clone();
    let poller = extra.create_write_poller().unwrap();
    let local = grant.tx.local_addr().unwrap();
    assert_eq!(send(&grant.tx, &transmit(local, b"")).await.datagrams, 1);
    let mut bytes = [0; 1];
    let mut buffers = [IoSliceMut::new(&mut bytes)];
    let mut meta = [MetadataReceive::default()];
    assert_eq!(
        poll_fn(|cx| grant.rx.poll_recv(cx, &mut buffers, &mut meta))
            .await
            .unwrap(),
        1
    );
    assert_eq!(meta[0].len, 0);
    drop(grant.rx);
    drop(grant.tx);
    assert!(
        claim.try_closed().is_none(),
        "original sender clone and poller still exist"
    );
    drop(extra);
    assert!(
        claim.try_closed().is_none(),
        "actual original poller still exists"
    );
    drop(poller);
    let receipt = claim.try_closed().unwrap();
    assert!(issuer.validates(&receipt, &scope.registrar()));
    let foreign = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    assert!(!issuer.validates(&receipt, &foreign.registrar()));
    assert_eq!(owner.occupied_main(), 1);
    assert!(
        grant.control.shared.socket.local_addr().is_ok(),
        "view receipt is no native close"
    );
    grant.control.close().unwrap();
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}

#[tokio::test]
async fn actual_same_numeric_reservations_cannot_replay_another_original_view_receipt() {
    let scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut first = MetadataOwner::new(limits()).unwrap();
    let mut a = first.bind(address()).unwrap();
    let mut second = MetadataOwner::new(limits()).unwrap();
    let mut b = second.bind(address()).unwrap();
    assert_eq!(a.control.reservation(), b.control.reservation());
    let (issuer_a, claim_a) = a
        .control
        .claim_views(scope.registrar().scope_identity())
        .unwrap();
    let (issuer_b, claim_b) = b
        .control
        .claim_views(scope.registrar().scope_identity())
        .unwrap();
    drop(a.rx);
    drop(a.tx);
    drop(b.rx);
    drop(b.tx);
    let receipt_a = claim_a.try_closed().unwrap();
    let receipt_b = claim_b.try_closed().unwrap();
    assert!(issuer_a.validates(&receipt_a, &scope.registrar()));
    assert!(issuer_b.validates(&receipt_b, &scope.registrar()));
    assert!(!issuer_a.validates(&receipt_b, &scope.registrar()));
    assert!(!issuer_b.validates(&receipt_a, &scope.registrar()));
    assert_eq!(first.close_all().main.confirmed_closed, 1);
    assert_eq!(second.close_all().main.confirmed_closed, 1);
}
struct LastWaker {
    claim: Arc<MetadataViewClaim>,
    dropped: Arc<AtomicUsize>,
    wakes: Arc<AtomicUsize>,
}
impl Wake for LastWaker {
    fn wake(self: Arc<Self>) {
        assert!(
            self.claim.try_closed().is_none(),
            "native wake cannot release the original RX view"
        );
        self.wakes.fetch_add(1, Ordering::SeqCst);
    }
}
impl Drop for LastWaker {
    fn drop(&mut self) {
        assert!(
            self.claim.try_closed().is_none(),
            "original caller waker drops before RX view capacity is returned"
        );
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn actual_pending_rx_last_caller_waker_drop_precedes_view_receipt_even_after_native_close() {
    let scope = TaskScope::new(tokio::runtime::Handle::current(), bound(1));
    let mut owner = MetadataOwner::new(limits()).unwrap();
    let mut grant = owner.bind(address()).unwrap();
    let (issuer, claim) = grant
        .control
        .claim_views(scope.registrar().scope_identity())
        .unwrap();
    let claim = Arc::new(claim);
    let dropped = Arc::new(AtomicUsize::new(0));
    let wakes = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(LastWaker {
        claim: claim.clone(),
        dropped: dropped.clone(),
        wakes: wakes.clone(),
    }));
    let mut bytes = [0; 1];
    let mut buffers = [IoSliceMut::new(&mut bytes)];
    let mut meta = [MetadataReceive::default()];
    assert!(grant
        .rx
        .poll_recv(&mut Context::from_waker(&waker), &mut buffers, &mut meta)
        .is_pending());
    drop(waker);
    drop(grant.tx);
    // Explicit native close fences/wakes the original but does not free its
    // real outstanding caller-waker registration or turn it into a user ACK.
    grant.control.close().unwrap();
    assert!(wakes.load(Ordering::SeqCst) > 0);
    assert!(claim.try_closed().is_none());
    drop(grant.rx);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(issuer.validates(&claim.try_closed().unwrap(), &scope.registrar()));
    assert_eq!(owner.take_report().main.confirmed_closed, 1);
}
