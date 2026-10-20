use super::*;
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
#[derive(Default)]
struct WakeCount(AtomicUsize);
impl Wake for WakeCount {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn pending_with_wake<F: Future>(future: std::pin::Pin<&mut F>, wakes: &Arc<WakeCount>) {
    let waker = Waker::from(wakes.clone());
    assert!(future.poll(&mut Context::from_waker(&waker)).is_pending());
}

fn bound(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
fn address() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}
async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
#[tokio::test]
async fn clones_share_original_port_and_peer_drop_does_not_retire_listener() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut control) = owner.bind_listener(address(), bound(3)).unwrap();
    let local = rx.local_addr().unwrap();
    let first = tokio::net::UdpSocket::bind(address()).await.unwrap();
    let second = tokio::net::UdpSocket::bind(address()).await.unwrap();
    let clone = tx.clone();
    let (a, b) = tokio::join!(
        tx.send_to_addr(b"", first.local_addr().unwrap()),
        clone.send_to_addr(b"two", second.local_addr().unwrap())
    );
    assert_eq!(a.unwrap(), 0);
    assert_eq!(b.unwrap(), 3);
    let mut bytes = [0; 16];
    assert_eq!(first.recv_from(&mut bytes).await.unwrap(), (0, local));
    let (size, source) = second.recv_from(&mut bytes).await.unwrap();
    assert_eq!(source, local);
    assert_eq!(&bytes[..size], b"two");
    drop(clone);
    first.send_to(b"still-live", local).await.unwrap();
    let (size, peer) = rx.recv_from_addr(&mut bytes).await.unwrap();
    assert_eq!(peer, first.local_addr().unwrap());
    assert_eq!(&bytes[..size], b"still-live");
    assert_eq!(owner.occupied(), 1);
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    let receipt = control.close().unwrap();
    assert_eq!(receipt.reservation(), control.reservation());
    assert_eq!(control.close().unwrap(), receipt);
    assert_eq!(owner.occupied(), 0);
    assert_eq!(owner.take_report().confirmed_closed, 1);
    // Live old clones cannot cross into a newly bound generation of the port.
    let replacement = tokio::net::UdpSocket::bind(local).await.unwrap();
    assert_eq!(
        tx.send_to_addr(b"old", local).await.unwrap_err().kind(),
        std_io::ErrorKind::NotConnected
    );
    assert_eq!(
        rx.recv_from_addr(&mut bytes).await.unwrap_err().kind(),
        std_io::ErrorKind::NotConnected
    );
    drop(replacement);
}
#[tokio::test]
async fn close_wakes_native_writer_lock_waiter_and_unique_receiver_without_new_tasks() {
    let target = tokio::net::UdpSocket::bind(address()).await.unwrap();
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut control) = owner.bind_listener(address(), bound(2)).unwrap();
    let local = tx.local_addr().unwrap();
    let clone = tx.clone();
    let peer = target.local_addr().unwrap();
    // On this current-thread runtime no reactor turn has occurred since bind:
    // the actual native poll installs the sole Tokio writer waker and suspends.
    let first = tx.send_to_addr(b"one", peer);
    tokio::pin!(first);
    let first_wake = Arc::new(WakeCount::default());
    pending_with_wake(first.as_mut(), &first_wake);
    let second = clone.send_to_addr(b"two", peer);
    tokio::pin!(second);
    let second_wake = Arc::new(WakeCount::default());
    pending_with_wake(second.as_mut(), &second_wake);
    assert_eq!(tx.shared.sends.available_permits(), 0);
    assert_eq!(
        tx.send_to_addr(b"full", peer).await.unwrap_err().kind(),
        std_io::ErrorKind::WouldBlock
    );
    let mut bytes = [0; 8];
    let read = rx.recv_from_addr(&mut bytes);
    tokio::pin!(read);
    let read_wake = Arc::new(WakeCount::default());
    pending_with_wake(read.as_mut(), &read_wake);
    control.close().unwrap();
    for wakes in [&first_wake, &second_wake, &read_wake] {
        assert!(
            wakes.0.load(Ordering::SeqCst) > 0,
            "close must wake every suspended view"
        );
    }
    let (first, second, read) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(first, second, read)
    })
    .await
    .unwrap();
    for error in [first.unwrap_err(), second.unwrap_err(), read.unwrap_err()] {
        assert_eq!(error.kind(), std_io::ErrorKind::NotConnected);
    }
    assert_eq!(tx.shared.sends.available_permits(), 2);
    assert_eq!(owner.take_report().confirmed_closed, 1);
    tokio::net::UdpSocket::bind(local).await.unwrap();
}
#[tokio::test]
async fn cancelled_send_refunds_only_its_borrow_and_revoke_keeps_original_until_close() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
    let local = tx.local_addr().unwrap();
    // An actual writer lock prevents a second native writer poll. Cancelling
    // the waiting caller returns its admission, with no global cancellation.
    let writer = tx.shared.writer.lock().await;
    let mut send = Box::pin(tx.send_to_addr(b"borrowed", local));
    pending(send.as_mut()).await;
    assert_eq!(tx.shared.sends.available_permits(), 0);
    drop(send);
    assert_eq!(tx.shared.sends.available_permits(), 1);
    drop(writer);
    tx.send_to_addr(b"after-cancel", local).await.unwrap();
    let mut bytes = [0; 32];
    assert_eq!(rx.recv_from_addr(&mut bytes).await.unwrap().0, 12);
    control.revoke();
    assert_eq!(owner.occupied(), 1);
    assert_eq!(owner.take_report().confirmed_closed, 0);
    assert!(tokio::net::UdpSocket::bind(local).await.is_err());
    control.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
    tokio::net::UdpSocket::bind(local).await.unwrap();
}
#[tokio::test]
async fn owner_shutdown_wakes_retained_receiver_and_failed_exact_bind_has_real_close_receipt() {
    let mut owner = ControlledDatagramOwner::new(bound(2));
    let (mut rx, tx, control) = owner.bind_listener(address(), bound(1)).unwrap();
    let local = tx.local_addr().unwrap();
    let error = match owner.bind_listener(local, bound(1)) {
        Ok(_) => panic!("exact listener bind must not fall back to another port"),
        Err(error) => error,
    };
    assert!(matches!(error.outcome, AcquisitionOutcome::Closed(_)));
    assert_eq!(owner.take_report().confirmed_closed, 1);
    let mut bytes = [0; 1];
    let read = rx.recv_from_addr(&mut bytes);
    tokio::pin!(read);
    pending(read.as_mut()).await;
    drop(control); // Controller Drop still is not native close or revocation.
    assert!(!tx.shared.socket.entry.retired.is_cancelled());
    let report = owner.close_all();
    assert!(report.is_complete());
    assert_eq!(report.confirmed_closed, 1);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), read)
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::NotConnected
    );
    tokio::net::UdpSocket::bind(local).await.unwrap();
}
#[test]
fn actual_registration_panic_retains_unknown_source_and_charged_original() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = owner.bind_listener(address(), bound(1));
    }))
    .is_err());
    let first = owner.close_all();
    let retry = owner.close_all();
    assert!(!first.is_complete());
    assert_eq!(owner.occupied(), 1);
    assert_eq!(first.confirmed_closed, 0);
    assert_eq!(retry.unconfirmed.len(), 1);
    assert!(Arc::ptr_eq(
        &first.unconfirmed[0].source,
        &retry.unconfirmed[0].source
    ));
    let denied = match owner.bind_listener(address(), bound(1)) {
        Ok(_) => panic!("unknown original cannot be replaced"),
        Err(error) => error,
    };
    assert!(matches!(denied.outcome, AcquisitionOutcome::Unknown(_)));
    assert!(Arc::ptr_eq(&denied.source, &first.unconfirmed[0].source));
}
#[tokio::test]
async fn excessive_send_capacity_is_rejected_before_native_acquisition() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    assert!(owner.bind_listener(address(), bound(usize::MAX)).is_err());
    assert_eq!(owner.occupied(), 0);
    assert_eq!(owner.take_report().confirmed_closed, 0);
}

#[tokio::test]
async fn oversized_native_send_keeps_source_and_listener_healthy() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut control) = owner.bind_listener(address(), bound(1)).unwrap();
    let local = tx.local_addr().unwrap();
    let oversized = vec![0; 65_508];
    let error = tx.send_to_addr(&oversized, local).await.unwrap_err();
    assert!(
        error.raw_os_error().is_some(),
        "retain actual OS datagram error"
    );
    assert_eq!(tx.shared.sends.available_permits(), 1);
    tx.send_to_addr(b"", local).await.unwrap();
    assert_eq!(rx.recv_from_addr(&mut [0; 1]).await.unwrap(), (0, local));
    control.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
