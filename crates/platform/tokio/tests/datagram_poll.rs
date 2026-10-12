use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Context,
};
use zero_platform_tokio::TokioDatagramSocket;

#[derive(Debug, Default)]
struct Observation {
    bytes: AtomicUsize,
    packets: AtomicUsize,
    errors: AtomicUsize,
}
impl zero_traits::IoObserver for Observation {
    fn received(&self, bytes: usize) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    fn received_datagram(&self, bytes: usize) {
        self.received(bytes);
        self.packets.fetch_add(1, Ordering::Relaxed);
    }
    fn sent(&self, _: usize) {}
    fn error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }
    fn dropped(&self) {}
}

#[tokio::test]
async fn native_datagram_poll_observes_completion_once_and_not_pending_readiness() {
    let observer = Arc::new(Observation::default());
    let socket = TokioDatagramSocket::bind("127.0.0.1:0")
        .await
        .unwrap()
        .with_observer(Some(observer.clone()));
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let mut bytes = [0; 32];
    {
        let mut cx = Context::from_waker(std::task::Waker::noop());
        let mut receive = std::pin::pin!(poll_fn(|cx| socket.poll_recv_from_addr(cx, &mut bytes)));
        for _ in 0..3 {
            assert!(receive.as_mut().poll(&mut cx).is_pending());
        }
    }
    assert_eq!(observer.packets.load(Ordering::Relaxed), 0);
    assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
    for payload in [b"payload".as_slice(), &[]] {
        peer.send_to(payload, socket.local_addr().unwrap())
            .await
            .unwrap();
        let (size, sender) = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            poll_fn(|cx| socket.poll_recv_from_addr(cx, &mut bytes)),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(sender, peer.local_addr().unwrap());
        assert_eq!(&bytes[..size], payload);
    }
    assert_eq!(observer.bytes.load(Ordering::Relaxed), 7);
    assert_eq!(observer.packets.load(Ordering::Relaxed), 2);
    assert_eq!(observer.errors.load(Ordering::Relaxed), 0);
}
