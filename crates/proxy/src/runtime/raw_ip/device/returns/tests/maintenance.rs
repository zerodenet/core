use super::super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
struct Counters {
    rx: AtomicUsize,
    drops: AtomicUsize,
}
impl zero_traits::IoObserver for Counters {
    fn received(&self, _: usize) {
        self.rx.fetch_add(1, Ordering::Relaxed);
    }
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {
        self.drops.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn expired_burst_storage_is_reclaimed_without_removing_live_return_or_observation() {
    let routes = PacketReturns::default();
    let remote: IpAddr = "192.0.2.1".parse().unwrap();
    let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(2);
    let sender: zero_stack::packet_output::PacketSender = sender.into();
    let observer = Arc::new(Counters::default());
    let now = Instant::now();
    for n in 0..512u16 {
        let local = IpAddr::V4(std::net::Ipv4Addr::new(10, 0, (n >> 8) as u8, n as u8));
        let request = packet::build_udp(local, remote, 40000, 53, b"request");
        routes
            .register_observed(
                local,
                1,
                sender.clone(),
                Some(observer.clone()),
                packet::packet_conversation_key(&request),
            )
            .unwrap();
    }
    let live: IpAddr = "10.0.0.0".parse().unwrap();
    let live_key =
        packet::packet_conversation_key(&packet::build_udp(live, remote, 40000, 53, b"request"))
            .unwrap();
    let peak = routes.conversations.lock().unwrap().capacity();
    for (ip, route) in routes.routes.lock().unwrap().iter_mut() {
        if *ip != live {
            route.touched = now - Duration::from_secs(601);
        }
    }
    for (key, conversation) in routes.conversations.lock().unwrap().iter_mut() {
        if *key != live_key {
            conversation.touched = now - Duration::from_secs(601);
        }
    }
    for (key, observation) in routes.observations.lock().unwrap().iter_mut() {
        if *key != live_key {
            observation.touched = now - Duration::from_secs(601);
        }
    }
    routes.expire();
    assert_eq!(routes.routes.lock().unwrap().len(), 1);
    assert_eq!(routes.conversations.lock().unwrap().len(), 1);
    assert_eq!(routes.observations.lock().unwrap().len(), 1);
    assert!(routes.routes.lock().unwrap().capacity() < peak / 4);
    assert!(routes.conversations.lock().unwrap().capacity() < peak / 4);
    assert!(routes.observations.lock().unwrap().capacity() < peak / 4);
    eprintln!(
        "return table capacity: peak={peak} after={}",
        routes.conversations.lock().unwrap().capacity()
    );
    let reply = packet::build_udp(remote, live, 53, 40000, b"reply");
    assert!(routes.deliver(&reply));
    assert!(receiver.try_recv().is_ok());
    assert_eq!(observer.rx.load(Ordering::Relaxed), 1);
    assert_eq!(observer.drops.load(Ordering::Relaxed), 0);
    routes.clear();
    assert_eq!(routes.routes.lock().unwrap().capacity(), 0);
    assert_eq!(routes.conversations.lock().unwrap().capacity(), 0);
    assert_eq!(routes.observations.lock().unwrap().capacity(), 0);
}

#[tokio::test]
async fn maintenance_preserves_closed_unexpired_returns_and_discard_accounting() {
    let routes = PacketReturns::default();
    let local: IpAddr = "10.0.0.1".parse().unwrap();
    let remote: IpAddr = "192.0.2.1".parse().unwrap();
    let (sender, receiver) = mpsc::channel::<Vec<u8>>(1);
    let observer = Arc::new(Counters::default());
    let request = packet::build_udp(local, remote, 40000, 53, b"request");
    routes
        .register_observed(
            local,
            1,
            sender.into(),
            Some(observer.clone()),
            packet::packet_conversation_key(&request),
        )
        .unwrap();
    drop(receiver);
    routes.expire();
    let reply = packet::build_udp(remote, local, 53, 40000, b"reply");
    assert!(routes.deliver(&reply));
    assert_eq!(observer.rx.load(Ordering::Relaxed), 1);
    assert_eq!(observer.drops.load(Ordering::Relaxed), 1);
}
