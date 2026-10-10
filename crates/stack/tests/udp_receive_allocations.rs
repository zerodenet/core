use super::allocation::measure;
use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use zero_stack::{packet, ClientUdpStack};
use zero_traits::{IoObserver, PacketDropReason};

#[derive(Debug, Default)]
struct Observer {
    received: AtomicU64,
    full: AtomicU64,
    closed: AtomicU64,
}
impl IoObserver for Observer {
    fn received(&self, bytes: usize) {
        self.received.fetch_add(bytes as u64, Ordering::Relaxed);
    }
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {
        panic!("expected classified discard");
    }
    fn dropped_reason(&self, reason: PacketDropReason) {
        match reason {
            PacketDropReason::QueueFull => {
                self.full.fetch_add(1, Ordering::Relaxed);
            }
            PacketDropReason::QueueClosed => {
                self.closed.fetch_add(1, Ordering::Relaxed);
            }
            _ => panic!("unexpected discard reason"),
        }
    }
}
fn ip(value: &str) -> IpAddr {
    value.parse().unwrap()
}

#[tokio::test]
async fn ordinary_udp_delivery_allocates_only_one_payload_image_for_both_ip_families() {
    for (local, remote) in [("10.0.0.1", "10.0.0.2"), ("fd00::1", "fd00::2")] {
        let (sender, mut packets) = mpsc::channel(2);
        let stack = ClientUdpStack::new(vec![ip(local)], sender, 1420).unwrap();
        let observer = Arc::new(Observer::default());
        let mut socket = stack
            .bind_observed(ip(local), Some(observer.clone()))
            .unwrap();
        let remote = packet::Endpoint {
            ip: ip(remote),
            port: 53,
        };
        socket.send_to(b"request", remote).await.unwrap();
        packets.recv().await.unwrap();
        let payload = vec![0x42; 4096];
        let reply = packet::build_udp(
            remote.ip,
            ip(local),
            remote.port,
            socket.local_endpoint().port,
            &payload,
        );
        let (consumed, counts) = measure(payload.len(), || stack.feed_correlated(&reply));
        assert!(consumed);
        assert_eq!(counts.large_allocations, 1, "{counts:?}");
        assert_eq!(counts.large_bytes, payload.len());
        let received = socket.recv_from().await.unwrap();
        assert_eq!(received.source, remote);
        assert_eq!(received.payload, payload);
        assert_eq!(
            observer.received.load(Ordering::Relaxed),
            reply.len() as u64
        );
        assert_eq!(observer.full.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn saturated_udp_receive_retries_once_with_one_payload_copy_and_preserves_discard_and_correlation(
) {
    let (sender, mut packets) = mpsc::channel(2);
    let observer = Arc::new(Observer::default());
    let stack = ClientUdpStack::new_observed_with_drops(
        vec![ip("10.0.0.1")],
        sender,
        1420,
        Some(observer.clone()),
    )
    .unwrap();
    let mut socket = stack
        .bind_observed(ip("10.0.0.1"), Some(observer.clone()))
        .unwrap();
    let remote = packet::Endpoint {
        ip: ip("10.0.0.2"),
        port: 53,
    };
    socket.send_to(b"request", remote).await.unwrap();
    packets.recv().await.unwrap();
    let reply = packet::build_udp(
        remote.ip,
        socket.local_endpoint().ip,
        remote.port,
        socket.local_endpoint().port,
        &[0x42; 4096],
    );
    for _ in 0..64 {
        assert!(stack.feed_correlated(&reply));
    }
    let (consumed, counts) = measure(1, || stack.feed_correlated(&reply));
    assert!(
        consumed,
        "refused correlated reply cannot become new ingress"
    );
    assert_eq!(counts.large_allocations, 1);
    assert_eq!(counts.large_bytes, 4096);
    let (accepted, counts) = measure(1, || stack.feed(&reply));
    assert!(
        !accepted,
        "unconnected delivery still reports queue refusal"
    );
    assert_eq!(counts.large_allocations, 1);
    assert_eq!(counts.large_bytes, 4096);
    assert_eq!(
        observer.full.load(Ordering::Relaxed),
        2,
        "shared boundary observer must not duplicate discards"
    );
    assert_eq!(
        observer.received.load(Ordering::Relaxed),
        reply.len() as u64 * 66
    );
    assert_eq!(socket.recv_from().await.unwrap().payload.len(), 4096);
    assert!(
        stack.feed_correlated(&reply),
        "queue reservation recovers after a read"
    );
}

#[tokio::test]
async fn unmatched_and_invalid_udp_packets_allocate_no_packet_or_payload_image() {
    let (sender, _packets) = mpsc::channel(1);
    let stack = ClientUdpStack::new(vec![ip("10.0.0.1")], sender, 1420).unwrap();
    let socket = stack.bind(ip("10.0.0.1")).unwrap();
    let reply = packet::build_udp(
        ip("10.0.0.2"),
        socket.local_endpoint().ip,
        53,
        socket.local_endpoint().port,
        &[0x42; 4096],
    );
    // Exclude one-time platform mutex initialization from steady-state packet
    // admission. In particular, macOS allocates the fragment mutex on first use.
    stack.cleanup_expired(std::time::Instant::now());
    for input in [reply.as_slice(), b"invalid".as_slice()] {
        let (consumed, counts) = measure(1, || stack.feed_correlated(input));
        assert!(!consumed);
        assert_eq!(counts.allocations, 0, "{counts:?}");
    }
}
