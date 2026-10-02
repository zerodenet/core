use std::{
    net::{IpAddr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    sync::mpsc,
    time::{timeout, Duration},
};
use zero_stack::{packet, packet_output::ObservedPacket, ClientTcpStack, ClientUdpStack};
use zero_traits::IoObserver;

#[derive(Debug, Default)]
struct Counts {
    rx: AtomicU64,
    tx: AtomicU64,
    full: AtomicU64,
    closed: AtomicU64,
}
impl IoObserver for Counts {
    fn received(&self, n: usize) {
        self.rx.fetch_add(n as u64, Ordering::Relaxed);
    }
    fn sent(&self, n: usize) {
        self.tx.fetch_add(n as u64, Ordering::Relaxed);
    }
    fn error(&self) {}
    fn dropped(&self) {}
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        match reason {
            zero_traits::PacketDropReason::QueueFull => {
                self.full.fetch_add(1, Ordering::Relaxed);
            }
            zero_traits::PacketDropReason::QueueClosed => {
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
async fn udp_aliases_preserve_fragment_provenance_and_count_only_correlated_rx() {
    let (sender, mut packets) = mpsc::channel::<ObservedPacket>(32);
    let stack = ClientUdpStack::new_observed(vec![ip("10.0.0.1")], sender, 100).unwrap();
    let first = Arc::new(Counts::default());
    let second = Arc::new(Counts::default());
    let a = stack
        .bind_observed(ip("10.0.0.1"), Some(first.clone()))
        .unwrap();
    let b = stack
        .bind_observed(ip("10.0.0.1"), Some(second.clone()))
        .unwrap();
    let remote = packet::Endpoint {
        ip: ip("10.0.0.2"),
        port: 53,
    };
    a.send_to(&[1; 200], remote).await.unwrap();
    let mut fragments = 0;
    while let Ok(packet) = packets.try_recv() {
        let observer = packet.observer.unwrap();
        assert!(Arc::ptr_eq(
            &observer,
            &(first.clone() as Arc<dyn IoObserver>)
        ));
        assert!(packet.packet.len() <= 100);
        // Queue admission alone must not count as completed device transmission.
        assert_eq!(first.tx.load(Ordering::Relaxed), 0);
        fragments += 1;
    }
    assert!(fragments > 1);
    b.send_to(b"second", remote).await.unwrap();
    let packet = packets.recv().await.unwrap();
    assert!(Arc::ptr_eq(
        &packet.observer.unwrap(),
        &(second.clone() as Arc<dyn IoObserver>)
    ));
    let reply = packet::build_udp(
        remote.ip,
        a.local_endpoint().ip,
        remote.port,
        a.local_endpoint().port,
        b"first",
    );
    assert!(stack.feed_correlated(&reply));
    assert_eq!(first.rx.load(Ordering::Relaxed), reply.len() as u64);
    assert_eq!(second.rx.load(Ordering::Relaxed), 0);
    let stranger = packet::build_udp(
        ip("10.0.0.3"),
        b.local_endpoint().ip,
        remote.port,
        b.local_endpoint().port,
        b"stray",
    );
    assert!(!stack.feed_correlated(&stranger));
    assert_eq!(second.rx.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn tcp_syn_retransmission_ack_and_reset_keep_the_flow_observer() {
    let local = ip("10.0.0.1");
    let remote: SocketAddr = "10.0.0.2:80".parse().unwrap();
    let (sender, mut packets) = mpsc::channel::<ObservedPacket>(32);
    let stack = Arc::new(ClientTcpStack::new_observed(vec![local], sender, 1420).unwrap());
    let counter = Arc::new(Counts::default());
    let connecting = {
        let stack = stack.clone();
        let counter = counter.clone();
        tokio::spawn(async move { stack.connect_observed(local, remote, Some(counter)).await })
    };
    let first = timeout(Duration::from_secs(1), packets.recv())
        .await
        .unwrap()
        .unwrap();
    let syn = packet::parse_tcp(&first.packet).unwrap();
    let retry = timeout(Duration::from_secs(3), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retry.packet, first.packet);
    assert!(Arc::ptr_eq(
        &retry.observer.unwrap(),
        &first.observer.unwrap()
    ));
    let reply = packet::build_tcp(
        remote.ip(),
        local,
        remote.port(),
        syn.src.port,
        100,
        syn.seq.wrapping_add(1),
        packet::tcp_flags::SYN | packet::tcp_flags::ACK,
        &[],
    );
    stack.feed(&reply).await;
    let stream = connecting.await.unwrap().unwrap();
    let ack = timeout(Duration::from_secs(1), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(
        &ack.observer.unwrap(),
        &(counter.clone() as Arc<dyn IoObserver>)
    ));
    assert_eq!(counter.rx.load(Ordering::Relaxed), reply.len() as u64);
    drop(stream);
    let reset = timeout(Duration::from_secs(1), packets.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(packet::parse_tcp(&reset.packet).unwrap().rst);
    assert!(Arc::ptr_eq(
        &reset.observer.unwrap(),
        &(counter.clone() as Arc<dyn IoObserver>)
    ));
}

#[tokio::test]
async fn full_correlated_udp_queue_does_not_reclassify_outbound_reply_as_inbound() {
    let (sender, mut packets) = mpsc::channel::<ObservedPacket>(2);
    let stack = ClientUdpStack::new_observed(vec![ip("10.0.0.1")], sender, 1420).unwrap();
    let counter = Arc::new(Counts::default());
    let socket = stack
        .bind_observed(ip("10.0.0.1"), Some(counter.clone()))
        .unwrap();
    let remote = packet::Endpoint {
        ip: ip("10.0.0.2"),
        port: 53,
    };
    socket.send_to(b"query", remote).await.unwrap();
    packets.recv().await.unwrap();
    let reply = packet::build_udp(
        remote.ip,
        socket.local_endpoint().ip,
        remote.port,
        socket.local_endpoint().port,
        b"reply",
    );
    for _ in 0..65 {
        assert!(stack.feed_correlated(&reply));
    }
    assert_eq!(counter.rx.load(Ordering::Relaxed), 65 * reply.len() as u64);
    assert_eq!(counter.full.load(Ordering::Relaxed), 1);
    let other = packet::build_udp(
        ip("10.0.0.3"),
        socket.local_endpoint().ip,
        remote.port,
        socket.local_endpoint().port,
        b"unrelated",
    );
    assert!(!stack.feed_correlated(&other));
    assert_eq!(counter.rx.load(Ordering::Relaxed), 65 * reply.len() as u64);
    assert_eq!(counter.full.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn output_queue_refusal_returns_ownership_and_does_not_claim_a_discard() {
    let counter = Arc::new(Counts::default());
    let (tx, rx) = mpsc::channel::<ObservedPacket>(1);
    let output =
        zero_stack::packet_output::PacketSender::from(tx).with_observer(Some(counter.clone()));
    output.try_send(vec![1]).unwrap();
    assert_eq!(counter.full.load(Ordering::Relaxed), 0);
    assert!(output.try_send(vec![2]).is_err());
    assert_eq!(counter.full.load(Ordering::Relaxed), 0);
    drop(rx);
    assert!(output.send(vec![3]).await.is_err());
    assert_eq!(counter.closed.load(Ordering::Relaxed), 0);
    assert_eq!(counter.tx.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn closing_udp_socket_counts_received_packets_still_waiting_for_the_consumer() {
    let (sender, mut output) = mpsc::channel::<ObservedPacket>(1);
    let device = Arc::new(Counts::default());
    let role = Arc::new(Counts::default());
    let stack = ClientUdpStack::new_observed_with_drops(
        vec![ip("10.0.0.1")],
        sender,
        1420,
        Some(device.clone()),
    )
    .unwrap();
    let socket = stack
        .bind_observed(ip("10.0.0.1"), Some(role.clone()))
        .unwrap();
    let remote = packet::Endpoint {
        ip: ip("10.0.0.2"),
        port: 53,
    };
    socket.send_to(b"q", remote).await.unwrap();
    output.recv().await.unwrap();
    let reply = packet::build_udp(
        remote.ip,
        socket.local_endpoint().ip,
        remote.port,
        socket.local_endpoint().port,
        b"r",
    );
    assert!(stack.feed_correlated(&reply));
    assert!(stack.feed_correlated(&reply));
    drop(socket);
    assert_eq!(role.closed.load(Ordering::Relaxed), 2);
    assert_eq!(device.closed.load(Ordering::Relaxed), 2);
}
