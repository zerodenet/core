use super::*;
use std::sync::Arc;
use zero_stack::packet;
fn options(
    routes: usize,
    conversations: usize,
    observations: usize,
) -> crate::runtime::raw_ip::RawIpStackOptions {
    let mut config = zero_config::RuntimeOptionsConfig::default();
    config.packet_return_max_routes = Some(routes);
    config.packet_return_max_conversations = Some(conversations);
    config.packet_return_max_observations = Some(observations);
    crate::runtime::raw_ip::RawIpStackOptions::default().with_packet_return_rows(&config)
}
#[tokio::test]
async fn configured_return_rows_reject_only_new_work_and_keep_original_paid_tables() {
    let options = options(1, 1, 1);
    let root = options.packet_return_totals.clone();
    let routes = PacketReturns::new_for_options(&options, false, None).unwrap();
    let baseline = root.driver_metadata().occupied;
    let local = "10.0.0.1".parse().unwrap();
    let remote = "192.0.2.1".parse().unwrap();
    let key = packet::packet_conversation_key(&packet::build_udp(local, remote, 40000, 53, b"q"))
        .unwrap();
    let (send, mut receive) = mpsc::channel::<Vec<u8>>(2);
    let send: zero_stack::packet_output::PacketSender = send.into();
    routes
        .register_observed(local, 1, send.clone(), None, Some(key))
        .unwrap();
    assert!(root.driver_metadata().occupied.bytes() > baseline.bytes());
    let second =
        packet::packet_conversation_key(&packet::build_udp(local, remote, 40001, 53, b"new"));
    assert_eq!(
        routes
            .register_observed(local, 1, send.clone(), None, second)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(
        routes
            .register("10.0.0.2".parse().unwrap(), 2, send)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    assert_eq!(routes.routes.lock().unwrap().len(), 1);
    assert_eq!(routes.conversations.lock().unwrap().len(), 1);
    assert!(routes.deliver(&packet::build_udp(remote, local, 53, 40000, b"original")));
    assert_eq!(
        packet::parse_udp(&receive.recv().await.unwrap())
            .unwrap()
            .payload,
        b"original"
    );
    routes.clear();
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[derive(Debug, Default)]
struct Coverage(std::sync::atomic::AtomicUsize);
impl zero_traits::IoObserver for Coverage {
    fn received(&self, _: usize) {}
    fn sent(&self, _: usize) {}
    fn error(&self) {}
    fn dropped(&self) {}
    fn receive_coverage_lost(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
#[test]
fn configured_observation_limit_loses_only_new_metrics_not_valid_return_execution() {
    let options = options(1, 2, 1);
    let routes = PacketReturns::new_for_options(&options, false, None).unwrap();
    let local = "10.0.0.1".parse().unwrap();
    let remote = "192.0.2.1".parse().unwrap();
    let observer = Arc::new(Coverage::default());
    let (send, mut receive) = mpsc::channel::<Vec<u8>>(2);
    let send: zero_stack::packet_output::PacketSender = send.into();
    for port in [40000, 40001] {
        let key =
            packet::packet_conversation_key(&packet::build_udp(local, remote, port, 53, b"q"));
        routes
            .register_observed(local, 1, send.clone(), Some(observer.clone()), key)
            .unwrap();
        assert!(routes.deliver(&packet::build_udp(remote, local, 53, port, b"valid")));
        assert!(receive.try_recv().is_ok());
    }
    assert_eq!(routes.observations.lock().unwrap().len(), 1);
    assert_eq!(routes.conversations.lock().unwrap().len(), 2);
    assert_eq!(observer.0.load(std::sync::atomic::Ordering::Relaxed), 1);
}
#[tokio::test]
async fn aged_open_original_stays_deliverable_and_capacity_cannot_replace_it() {
    let options = options(1, 1, 1);
    let routes = PacketReturns::new_for_options(&options, false, None).unwrap();
    let local = "10.0.0.1".parse().unwrap();
    let remote = "192.0.2.1".parse().unwrap();
    let key = packet::packet_conversation_key(&packet::build_udp(local, remote, 40000, 53, b"q"));
    let observer = Arc::new(Rx::default());
    let (send, mut receive) = mpsc::channel::<Vec<u8>>(2);
    let send: zero_stack::packet_output::PacketSender = send.into();
    routes
        .register_observed(local, 1, send.clone(), Some(observer.clone()), key)
        .unwrap();
    let old = std::time::Instant::now() - std::time::Duration::from_secs(601);
    routes.routes.lock().unwrap()[0].1.touched = old;
    routes.conversations.lock().unwrap()[0].1.touched = old;
    routes.observations.lock().unwrap()[0].1.touched = old;
    routes.expire();
    assert_eq!(routes.routes.lock().unwrap().len(), 1);
    assert_eq!(routes.conversations.lock().unwrap().len(), 1);
    assert_eq!(routes.observations.lock().unwrap().len(), 1);
    let (other, _held) = mpsc::channel::<Vec<u8>>(1);
    assert_eq!(
        routes.register(local, 1, other.into()).unwrap_err().kind(),
        std::io::ErrorKind::AddrInUse
    );
    assert_eq!(
        routes
            .register("10.0.0.2".parse().unwrap(), 2, send.clone())
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    let second =
        packet::packet_conversation_key(&packet::build_udp(local, remote, 40001, 53, b"new"));
    assert_eq!(
        routes
            .register_observed(local, 1, send, None, second)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::WouldBlock
    );
    let reply = packet::build_udp(remote, local, 53, 40000, b"aged-valid");
    assert!(routes.deliver(&reply));
    assert_eq!(
        packet::parse_udp(&receive.recv().await.unwrap())
            .unwrap()
            .payload,
        b"aged-valid"
    );
    assert_eq!(
        observer.0.load(std::sync::atomic::Ordering::Relaxed),
        reply.len() as u64
    );
}
#[test]
fn actual_removed_observer_is_destroyed_only_after_all_return_owner_locks_release() {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Weak,
    };
    struct Probe {
        source: Weak<PacketReturns>,
        drops: Arc<AtomicUsize>,
    }
    impl std::fmt::Debug for Probe {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("actual return observer")
        }
    }
    impl zero_traits::IoObserver for Probe {
        fn received(&self, _: usize) {}
        fn sent(&self, _: usize) {}
        fn error(&self) {}
        fn dropped(&self) {}
    }
    impl Drop for Probe {
        fn drop(&mut self) {
            let source = self.source.upgrade().unwrap();
            assert!(source.routes.try_lock().is_ok());
            assert!(source.conversations.try_lock().is_ok());
            assert!(source.observations.try_lock().is_ok());
            self.drops.fetch_add(1, Ordering::Relaxed);
        }
    }
    for clear in [false, true] {
        let source = Arc::new(PacketReturns::default());
        let drops = Arc::new(AtomicUsize::new(0));
        let (send, mut receive) = mpsc::channel::<Vec<u8>>(1);
        let local = "10.0.0.1".parse().unwrap();
        let remote = "192.0.2.1".parse().unwrap();
        let key =
            packet::packet_conversation_key(&packet::build_udp(local, remote, 40000, 53, b"q"));
        source
            .register_observed(
                local,
                1,
                send.into(),
                Some(Arc::new(Probe {
                    source: Arc::downgrade(&source),
                    drops: drops.clone(),
                })),
                key,
            )
            .unwrap();
        receive.close();
        let old = std::time::Instant::now() - std::time::Duration::from_secs(601);
        source.routes.lock().unwrap()[0].1.touched = old;
        source.conversations.lock().unwrap()[0].1.touched = old;
        source.observations.lock().unwrap()[0].1.touched = old;
        if clear {
            source.clear();
        } else {
            source.expire();
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1);
    }
}
