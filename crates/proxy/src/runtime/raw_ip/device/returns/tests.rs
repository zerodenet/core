#[path = "tests/maintenance.rs"]
mod maintenance;

use std::net::{IpAddr, Ipv4Addr};

use tokio::sync::mpsc;

use super::PacketReturns;

#[tokio::test]
async fn owned_native_return_moves_buffer_and_returns_misses_unchanged() {
    use zero_stack::packet;
    let routes = PacketReturns::default();
    let local: IpAddr = "10.0.0.2".parse().unwrap();
    let reply = packet::build_udp("192.0.2.1".parse().unwrap(), local, 53, 40000, b"reply");
    let expected = reply.clone();
    let pointer = reply.as_ptr();
    let reply = routes.deliver_owned(reply.into()).unwrap_err();
    assert_eq!(reply.as_ptr(), pointer);
    assert_eq!(reply, expected);
    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(1);
    let sender: zero_stack::packet_output::PacketSender = sender.into();
    routes.register(local, 1, sender).unwrap();
    routes.deliver_owned(reply).unwrap();
    let received = receiver.recv().await.unwrap();
    assert_eq!(received.as_ptr(), pointer);
    assert_eq!(packet::ip_hop_limit(&received), Some(63));
    assert_eq!(packet::parse_udp(&received).unwrap().payload, b"reply");
    assert!(routes.deliver_owned(expected.clone().into()).is_ok());
    // A full queue still consumes and discards a native return. It must never
    // be presented again as a remote-initiated business packet.
    assert!(routes.deliver_owned(expected.clone().into()).is_ok());
    drop(receiver);
    assert!(routes.deliver_owned(expected.into()).is_ok());
}

#[test]
fn packet_returns_reject_overlapping_ingress_sources() {
    let routes = PacketReturns::default();
    let source = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    let (first, _first_rx) = mpsc::channel::<Vec<u8>>(1);
    let first: zero_stack::packet_output::PacketSender = first.into();
    let (second, _second_rx) = mpsc::channel::<Vec<u8>>(1);
    let second: zero_stack::packet_output::PacketSender = second.into();
    let first_id = crate::runtime::packet_route::next_ingress_id();
    let second_id = crate::runtime::packet_route::next_ingress_id();
    routes.register(source, first_id, first).unwrap();
    let error = routes.register(source, second_id, second).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
}

#[tokio::test]
async fn packet_returns_deliver_by_original_destination() {
    let routes = PacketReturns::default();
    let source = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(1);
    let sender: zero_stack::packet_output::PacketSender = sender.into();
    routes.register(source, 1, sender).unwrap();
    let mut reply = vec![0_u8; 28];
    reply[0] = 0x45;
    reply[2..4].copy_from_slice(&28_u16.to_be_bytes());
    reply[8] = 64;
    reply[12..16].copy_from_slice(&[127, 0, 0, 1]);
    reply[16..20].copy_from_slice(&[10, 0, 0, 2]);
    let checksum = zero_stack::packet::checksum(&reply[..20]);
    reply[10..12].copy_from_slice(&checksum.to_be_bytes());
    // A previously registered IP return route must not bypass a subsequently
    // withdrawn inbound direction.
    assert!(!routes.deliver_correlated(&reply));
    assert!(receiver.try_recv().is_err());
    assert!(routes.deliver(&reply));
    let routed = receiver.recv().await.unwrap();
    assert_eq!(routed[8], 63);
    assert_eq!(&routed[12..20], &reply[12..20]);
}

#[derive(Debug, Default)]
struct Rx(std::sync::atomic::AtomicU64);
impl zero_traits::IoObserver for Rx {
    fn received(&self, size: usize) {
        self.0
            .fetch_add(size as u64, std::sync::atomic::Ordering::Relaxed);
    }
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
}
#[tokio::test]
async fn shared_packet_source_keeps_distinct_outbound_conversation_observers() {
    use std::{
        net::IpAddr,
        sync::{atomic::Ordering, Arc},
    };
    use zero_stack::packet;
    let routes = PacketReturns::default();
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let remote_a: IpAddr = "198.51.100.1".parse().unwrap();
    let remote_b: IpAddr = "198.51.100.2".parse().unwrap();
    let a = Arc::new(Rx::default());
    let b = Arc::new(Rx::default());
    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(8);
    let sender: zero_stack::packet_output::PacketSender = sender.into();
    let first = packet::build_udp(local, remote_a, 1234, 53, b"a");
    let second = packet::build_udp(local, remote_b, 1234, 53, b"second");
    routes
        .register_observed(
            local,
            1,
            sender.clone(),
            Some(a.clone()),
            packet::packet_conversation_key(&first),
        )
        .unwrap();
    routes
        .register_observed(
            local,
            1,
            sender.clone(),
            Some(b.clone()),
            packet::packet_conversation_key(&second),
        )
        .unwrap();
    let reply_a = packet::build_udp(remote_a, local, 53, 1234, b"a");
    let reply_b = packet::build_udp(remote_b, local, 53, 1234, b"second");
    assert!(routes.deliver(&reply_a));
    assert!(routes.deliver(&reply_b));
    receiver.recv().await.unwrap();
    receiver.recv().await.unwrap();
    assert_eq!(a.0.load(Ordering::Relaxed), reply_a.len() as u64);
    assert_eq!(b.0.load(Ordering::Relaxed), reply_b.len() as u64);
    let unrelated = packet::build_udp(remote_a, local, 53, 4321, b"unassigned");
    assert!(routes.deliver(&unrelated));
    assert_eq!(a.0.load(Ordering::Relaxed), reply_a.len() as u64);
    routes.clear();
    assert!(!routes.deliver(&reply_a));
}

#[tokio::test]
async fn rejected_ingress_cannot_steal_an_accepted_conversation_observer() {
    use std::sync::{atomic::Ordering, Arc};
    use zero_stack::packet;
    let routes = PacketReturns::default();
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let remote: IpAddr = "198.51.100.1".parse().unwrap();
    let first = Arc::new(Rx::default());
    let rejected = Arc::new(Rx::default());
    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(2);
    let sender: zero_stack::packet_output::PacketSender = sender.into();
    let (other, _other_rx) = mpsc::channel::<Vec<u8>>(2);
    let other: zero_stack::packet_output::PacketSender = other.into();
    let request = packet::build_udp(local, remote, 1234, 53, b"a");
    let key = packet::packet_conversation_key(&request);
    routes
        .register_observed(local, 1, sender, Some(first.clone()), key)
        .unwrap();
    assert_eq!(
        routes
            .register_observed(local, 2, other, Some(rejected.clone()), key)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::AddrInUse
    );
    let reply = packet::build_udp(remote, local, 53, 1234, b"a");
    assert!(routes.deliver(&reply));
    receiver.recv().await.unwrap();
    assert_eq!(first.0.load(Ordering::Relaxed), reply.len() as u64);
    assert_eq!(rejected.0.load(Ordering::Relaxed), 0);
}

#[test]
fn observation_capacity_preserves_forwarding_and_reports_incomplete_rx() {
    use std::sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    };
    use zero_stack::packet;
    #[derive(Debug, Default)]
    struct Observer {
        lost: AtomicU64,
    }
    impl zero_traits::IoObserver for Observer {
        fn receive_coverage_lost(&self) {
            self.lost.fetch_add(1, Ordering::Relaxed);
        }
        fn received(&self, _: usize) {}
        fn sent(&self, _: usize) {}
        fn error(&self) {}
        fn dropped(&self) {}
    }
    let routes = PacketReturns::default();
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let remote: IpAddr = "198.51.100.1".parse().unwrap();
    let observer = Arc::new(Observer::default());
    let (sender, _receiver) = mpsc::channel::<Vec<u8>>(1);
    let sender: zero_stack::packet_output::PacketSender = sender.into();
    for port in 1..=4097 {
        let packet = packet::build_udp(local, remote, port, 53, b"query");
        routes
            .register_observed(
                local,
                1,
                sender.clone(),
                Some(observer.clone()),
                packet::packet_conversation_key(&packet),
            )
            .unwrap();
    }
    assert_eq!(routes.observations.lock().unwrap().len(), 4096);
    assert_eq!(observer.lost.load(Ordering::Relaxed), 1);
}

#[derive(Debug, Default)]
struct Drops(std::sync::atomic::AtomicU64);
impl zero_traits::IoObserver for Drops {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    fn dropped_reason(&self, reason: zero_traits::PacketDropReason) {
        assert_eq!(reason, zero_traits::PacketDropReason::QueueFull);
        self.dropped();
    }
}
#[tokio::test]
async fn full_return_queue_is_a_real_discard_at_endpoint_and_role_boundaries() {
    use std::sync::{atomic::Ordering, Arc};
    use zero_stack::packet;
    let endpoint = Arc::new(Drops::default());
    let role = Arc::new(Drops::default());
    let routes = PacketReturns::with_drop_observer(Some(endpoint.clone()));
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let remote: IpAddr = "10.0.0.2".parse().unwrap();
    let sent = packet::build_udp(local, remote, 1234, 53, b"q");
    let (sender, _receiver) = mpsc::channel::<Vec<u8>>(1);
    let sender: zero_stack::packet_output::PacketSender = sender.into();
    routes
        .register_observed(
            local,
            1,
            sender,
            Some(role.clone()),
            packet::packet_conversation_key(&sent),
        )
        .unwrap();
    let reply = packet::build_udp(remote, local, 53, 1234, b"r");
    assert!(routes.deliver(&reply));
    assert!(routes.deliver(&reply)); // Consumed even on discard, never admitted as a new flow.
    assert_eq!(endpoint.0.load(Ordering::Relaxed), 1);
    assert_eq!(role.0.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn native_return_retains_external_owner_on_miss_and_through_delivery() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Owner(Vec<u8>, Arc<AtomicUsize>);
    impl AsRef<[u8]> for Owner {
        fn as_ref(&self) -> &[u8] {
            &self.0
        }
    }
    impl AsMut<[u8]> for Owner {
        fn as_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }
    }
    impl zero_traits::PacketStorage for Owner {}
    impl Drop for Owner {
        fn drop(&mut self) {
            self.1.fetch_add(1, Ordering::Relaxed);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let local: IpAddr = "10.0.0.2".parse().unwrap();
    let bytes =
        zero_stack::packet::build_udp("192.0.2.1".parse().unwrap(), local, 53, 40000, b"return");
    let pointer = bytes.as_ptr();
    let buffer = zero_traits::PacketBuffer::from_owner(Owner(bytes, drops.clone()));
    let routes = PacketReturns::default();
    let buffer = routes.deliver_owned(buffer).unwrap_err();
    assert_eq!(buffer.as_ptr(), pointer);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let (tx, mut rx) = mpsc::channel::<zero_traits::PacketBuffer>(1);
    let tx: zero_stack::packet_output::PacketSender = tx.into();
    routes.register(local, 1, tx).unwrap();
    routes.deliver_owned(buffer).unwrap();
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let reply = rx.recv().await.unwrap();
    assert_eq!(reply.as_ptr(), pointer);
    assert_eq!(zero_stack::packet::ip_hop_limit(&reply), Some(63));
    assert_eq!(
        zero_stack::packet::parse_udp(&reply).unwrap().payload,
        b"return"
    );
    drop(reply);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn expired_conversation_cannot_override_the_current_return_route_between_sweeps() {
    use std::time::{Duration, Instant};
    use zero_stack::packet;
    let routes = PacketReturns::default();
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let remote: IpAddr = "10.0.0.2".parse().unwrap();
    let request = packet::build_udp(local, remote, 40000, 53, b"request");
    let key = packet::packet_conversation_key(&request).unwrap();
    let (old, mut old_packets) = mpsc::channel::<Vec<u8>>(1);
    routes
        .register_observed(local, 1, old.into(), None, Some(key))
        .unwrap();
    let (current, mut current_packets) = mpsc::channel::<Vec<u8>>(1);
    routes.register(local, 1, current.into()).unwrap();
    routes
        .conversations
        .lock()
        .unwrap()
        .get_mut(&key)
        .unwrap()
        .touched = Instant::now() - Duration::from_secs(601);
    let reply = packet::build_udp(remote, local, 53, 40000, b"reply");
    assert!(routes.deliver_owned(reply.into()).is_ok());
    assert!(current_packets.try_recv().is_ok());
    assert!(old_packets.try_recv().is_err());
}
