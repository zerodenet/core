use super::*;
use crate::{contract::component::GrantedDatagramListener, PacketSocketReceiver};
use std::{
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

fn bound(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}
async fn pending<F: Future>(mut future: std::pin::Pin<&mut F>) {
    poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
#[derive(Debug, Default)]
struct Counts {
    boundaries: AtomicUsize,
    sent: AtomicUsize,
    received: AtomicUsize,
    errors: AtomicUsize,
}
impl zero_traits::IoObserver for Counts {
    fn datagram_boundary(&self) {
        self.boundaries.fetch_add(1, Ordering::SeqCst);
    }
    fn sent_datagram(&self, _: usize) {
        self.sent.fetch_add(1, Ordering::SeqCst);
    }
    fn received_datagram(&self, _: usize) {
        self.received.fetch_add(1, Ordering::SeqCst);
    }
    fn received(&self, _: usize) {
        unreachable!()
    }
    fn sent(&self, _: usize) {
        unreachable!()
    }
    fn error(&self) {
        self.errors.fetch_add(1, Ordering::SeqCst);
    }
    fn dropped(&self) {
        unreachable!()
    }
}
async fn roundtrip(address: &str) {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (rx, tx, mut native) = owner
        .bind_listener(address.parse().unwrap(), bound(2))
        .unwrap();
    let retained = tx.clone();
    let local = rx.local_addr().unwrap();
    let counts = Arc::new(Counts::default());
    native.set_observer(Some(counts.clone())).unwrap();
    let (mut receiver, mut sender) = PacketSocketReceiver::from_granted(
        GrantedDatagramListener { rx, tx },
        EgressInterfaceControl::default(),
    );
    let peer = tokio::net::UdpSocket::bind(address).await.unwrap();
    let remote = peer.local_addr().unwrap();
    for bytes in [b"payload".as_slice(), &[]] {
        assert_eq!(sender.send_to(bytes, remote).await.unwrap(), bytes.len());
        let mut buffer = [0; 16];
        let (n, source) = tokio::time::timeout(Duration::from_secs(1), peer.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&buffer[..n], bytes);
        assert_eq!(source, local);
        peer.send_to(bytes, local).await.unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), receiver.recv_from(&mut buffer))
                .await
                .unwrap()
                .unwrap(),
            (bytes.len(), remote)
        );
        assert_eq!(&buffer[..bytes.len()], bytes);
    }
    assert_eq!(counts.boundaries.load(Ordering::SeqCst), 1);
    assert_eq!(counts.sent.load(Ordering::SeqCst), 2);
    assert_eq!(counts.received.load(Ordering::SeqCst), 2);
    assert_eq!(counts.errors.load(Ordering::SeqCst), 0);
    drop(sender); // A packet writer is not the listener's close owner.
    retained.send_to_addr(b"still-live", remote).await.unwrap();
    let mut buffer = [0; 16];
    assert_eq!(peer.recv_from(&mut buffer).await.unwrap(), (10, local));
    assert_eq!(owner.occupied(), 1);
    let receipt = native.close().unwrap();
    assert_eq!(receipt.reservation(), native.reservation());
    assert_eq!(owner.take_report().confirmed_closed, 1);
    assert_eq!(
        retained
            .send_to_addr(b"old", remote)
            .await
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::NotConnected
    );
    assert_eq!(
        receiver.recv_from(&mut buffer).await.unwrap_err().kind(),
        std_io::ErrorKind::NotConnected
    );
    assert_eq!(owner.occupied(), 0);
}
#[tokio::test]
async fn granted_packet_ipv4_keeps_original_port_observation_and_host_control() {
    roundtrip("127.0.0.1:0").await;
}
#[tokio::test]
async fn granted_packet_ipv6_keeps_original_port_observation_and_host_control() {
    roundtrip("[::1]:0").await;
}
#[tokio::test]
async fn packet_writer_and_plain_clone_share_admission_and_close_wakes_pending_io() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut native) = owner
        .bind_listener("127.0.0.1:0".parse().unwrap(), bound(2))
        .unwrap();
    let clone = tx.clone();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let target = peer.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    let first = tx.send_packet(b"actual-native-poll", target, &control);
    tokio::pin!(first);
    // Current-thread reactor has not yet observed the newly registered socket.
    pending(first.as_mut()).await;
    let second = clone.send_to_addr(b"waiting-on-same-writer", target);
    tokio::pin!(second);
    pending(second.as_mut()).await;
    assert_eq!(tx.shared.sends.available_permits(), 0);
    assert_eq!(
        tx.send_packet(b"full", target, &control)
            .await
            .unwrap_err()
            .kind(),
        std_io::ErrorKind::WouldBlock
    );
    let mut buffer = [0; 32];
    let read = rx.recv_from_addr(&mut buffer);
    tokio::pin!(read);
    pending(read.as_mut()).await;
    native.close().unwrap();
    let (a, b, c) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(first, second, read)
    })
    .await
    .unwrap();
    for result in [a.map(|_| ()), b.map(|_| ()), c.map(|_| ())] {
        assert_eq!(result.unwrap_err().kind(), std_io::ErrorKind::NotConnected);
    }
    assert_eq!(tx.shared.sends.available_permits(), 2);
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
#[tokio::test]
async fn cancelled_packet_wait_refunds_borrow_without_retiring_original() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut native) = owner
        .bind_listener("127.0.0.1:0".parse().unwrap(), bound(1))
        .unwrap();
    let local = rx.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    let writer = tx.shared.writer.lock().await;
    let mut send = Box::pin(tx.send_packet(b"cancel", local, &control));
    pending(send.as_mut()).await;
    assert_eq!(tx.shared.sends.available_permits(), 0);
    drop(send);
    assert_eq!(tx.shared.sends.available_permits(), 1);
    drop(writer);
    tx.send_packet(b"healthy", local, &control).await.unwrap();
    let mut buffer = [0; 16];
    assert_eq!(rx.recv_from_addr(&mut buffer).await.unwrap(), (7, local));
    assert!(native.packet_failure().is_none());
    assert_eq!(owner.take_report().confirmed_closed, 0);
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}
#[tokio::test]
async fn packet_oversize_keeps_native_source_and_healthy_next_submission() {
    let mut owner = ControlledDatagramOwner::new(bound(1));
    let (mut rx, tx, mut native) = owner
        .bind_listener("127.0.0.1:0".parse().unwrap(), bound(1))
        .unwrap();
    let local = rx.local_addr().unwrap();
    let control = EgressInterfaceControl::default();
    let error = tx
        .send_packet(&vec![0; 65_536], local, &control)
        .await
        .unwrap_err();
    assert!(error.raw_os_error().is_some());
    assert_ne!(error.kind(), std_io::ErrorKind::WouldBlock);
    assert!(native.packet_failure().is_none());
    assert_eq!(owner.occupied(), 1);
    tx.send_packet(b"", local, &control).await.unwrap();
    assert_eq!(rx.recv_from_addr(&mut [0; 8]).await.unwrap(), (0, local));
    native.close().unwrap();
    assert_eq!(owner.take_report().confirmed_closed, 1);
}

mod egress;
