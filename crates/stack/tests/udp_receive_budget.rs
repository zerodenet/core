use std::{
    net::IpAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use zero_stack::{client_udp::ClientUdpReceiveQueueConfig, packet, ClientUdpStack};
use zero_traits::{IoObserver, PacketDropReason};
#[derive(Debug, Default)]
struct Drops {
    full: AtomicUsize,
    closed: AtomicUsize,
}
impl IoObserver for Drops {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {
        panic!("unclassified drop");
    }
    fn dropped_reason(&self, reason: PacketDropReason) {
        match reason {
            PacketDropReason::QueueFull => {
                self.full.fetch_add(1, Ordering::Relaxed);
            }
            PacketDropReason::QueueClosed => {
                self.closed.fetch_add(1, Ordering::Relaxed);
            }
            _ => panic!("unexpected drop"),
        }
    }
}
fn ip(v: &str) -> IpAddr {
    v.parse().unwrap()
}
fn reply(socket: &zero_stack::ClientUdpSocket, payload: &[u8]) -> Vec<u8> {
    let local = socket.local_endpoint();
    packet::build_udp(ip("10.0.0.2"), local.ip, 53, local.port, payload)
}
fn stack(config: ClientUdpReceiveQueueConfig, drops: Arc<Drops>) -> ClientUdpStack {
    ClientUdpStack::new_with_queue(
        vec![ip("10.0.0.1")],
        mpsc::channel::<Vec<u8>>(1).0.into(),
        1420,
        Some(drops),
        config,
    )
    .unwrap()
}
#[tokio::test]
async fn configured_slot_capacity_increases_admission_without_reducing_default_delivery() {
    for capacity in [1, 64, 128] {
        let drops = Arc::new(Drops::default());
        let stack = stack(
            ClientUdpReceiveQueueConfig {
                capacity,
                ..Default::default()
            },
            drops.clone(),
        );
        let mut socket = stack.bind(ip("10.0.0.1")).unwrap();
        for n in 0..capacity {
            assert!(stack.feed(&reply(&socket, &[n as u8])));
        }
        assert!(!stack.feed(&reply(&socket, b"excess")));
        assert_eq!(drops.full.load(Ordering::Relaxed), 1);
        for n in 0..capacity {
            assert_eq!(socket.recv_from().await.unwrap().payload, [n as u8]);
        }
        assert!(stack.feed(&reply(&socket, b"resumed")));
        assert_eq!(socket.recv_from().await.unwrap().payload, b"resumed");
    }
}
#[tokio::test]
async fn stack_budget_is_shared_across_sockets_and_released_on_socket_drop() {
    let drops = Arc::new(Drops::default());
    let stack = stack(
        ClientUdpReceiveQueueConfig {
            capacity: 8,
            max_payload_bytes: Some(65_527),
            max_stack_payload_bytes: Some(65_527),
        },
        drops.clone(),
    );
    let mut a = stack.bind(ip("10.0.0.1")).unwrap();
    let b = stack
        .bind_observed(ip("10.0.0.1"), Some(drops.clone()))
        .unwrap();
    let payload = vec![0x42; 40_000];
    assert!(stack.feed(&reply(&a, &payload)));
    assert!(!stack.feed(&reply(&b, &payload)));
    assert_eq!(
        drops.full.load(Ordering::Relaxed),
        1,
        "one discard despite shared observers"
    );
    let held = a.recv_from().await.unwrap();
    assert!(
        stack.feed(&reply(&b, &payload)),
        "application-owned data is no longer queued"
    );
    assert!(!stack.feed(&reply(&a, &payload)));
    drop(b);
    assert_eq!(drops.closed.load(Ordering::Relaxed), 1);
    assert!(
        stack.feed(&reply(&a, &payload)),
        "closed socket releases shared bytes"
    );
    assert_eq!(a.recv_from().await.unwrap().payload, held.payload);
}
#[tokio::test]
async fn per_socket_byte_budget_preserves_other_sockets_and_zero_length_datagrams() {
    let drops = Arc::new(Drops::default());
    let stack = stack(
        ClientUdpReceiveQueueConfig {
            capacity: 4,
            max_payload_bytes: Some(65_527),
            max_stack_payload_bytes: None,
        },
        drops.clone(),
    );
    let mut a = stack.bind(ip("10.0.0.1")).unwrap();
    let mut b = stack.bind(ip("10.0.0.1")).unwrap();
    let payload = vec![7; 65_507];
    assert!(stack.feed(&reply(&a, &payload)));
    assert!(
        stack.feed(&reply(&a, &[1; 20])),
        "exact byte boundary accepts"
    );
    assert!(
        stack.feed(&reply(&a, &[])),
        "zero-byte payload still needs a slot"
    );
    assert!(!stack.feed(&reply(&a, &[1])));
    assert!(
        stack.feed(&reply(&b, &payload)),
        "per-socket byte cap does not limit another socket"
    );
    assert_eq!(a.recv_from().await.unwrap().payload, payload);
    assert_eq!(a.recv_from().await.unwrap().payload, [1; 20]);
    assert!(a.recv_from().await.unwrap().payload.is_empty());
    assert_eq!(b.recv_from().await.unwrap().payload, payload);
    assert_eq!(drops.full.load(Ordering::Relaxed), 1);
}
#[test]
fn invalid_receive_queue_is_rejected_before_binding() {
    for config in [
        ClientUdpReceiveQueueConfig {
            capacity: 0,
            ..Default::default()
        },
        ClientUdpReceiveQueueConfig {
            capacity: 65_537,
            ..Default::default()
        },
        ClientUdpReceiveQueueConfig {
            max_payload_bytes: Some(65_526),
            ..Default::default()
        },
        ClientUdpReceiveQueueConfig {
            max_stack_payload_bytes: Some(0),
            ..Default::default()
        },
    ] {
        assert!(matches!(
            ClientUdpStack::new_with_queue(
                vec![ip("10.0.0.1")],
                mpsc::channel::<Vec<u8>>(1).0.into(),
                1420,
                None,
                config
            ),
            Err(zero_stack::ClientUdpStackError::InvalidReceiveQueue)
        ));
    }
}
