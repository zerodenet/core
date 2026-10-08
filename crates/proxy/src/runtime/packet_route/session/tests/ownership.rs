use super::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use zero_traits::{PacketBuffer, PacketStorage};

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
impl PacketStorage for Owner {
    fn into_vec(self: Box<Self>) -> Vec<u8> {
        panic!("managed native reply must retain storage")
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.1.fetch_add(1, Ordering::Relaxed);
    }
}
fn engine() -> zero_engine::Engine {
    zero_engine::Engine::new(
        zero_config::RuntimeConfig::parse(r#"{"route":{"rules":[],"final":{"type":"direct"}}}"#)
            .unwrap(),
    )
    .unwrap()
}
fn packet() -> Vec<u8> {
    zero_stack::packet::build_udp(
        "10.0.0.2".parse().unwrap(),
        "10.0.0.3".parse().unwrap(),
        40000,
        443,
        b"native",
    )
}

#[tokio::test]
async fn managed_reply_pump_keeps_external_storage_until_consumer_finishes() {
    let engine = engine();
    let mut pins = PacketSessionPins::default().managed(engine, "tun".into());
    let bytes = packet();
    let pointer = bytes.as_ptr();
    let drops = Arc::new(AtomicUsize::new(0));
    let (destination, mut consumer) = tokio::sync::mpsc::channel::<PacketBuffer>(1);
    let plane = PacketPlane::Packet("endpoint".into());
    let replies = pins
        .replies_for(&bytes, &plane, None, destination.into())
        .unwrap();
    pins.record(&bytes, plane);
    replies
        .send_buffer(PacketBuffer::from_owner(Owner(bytes, drops.clone())))
        .await
        .unwrap();
    let received = tokio::time::timeout(std::time::Duration::from_secs(1), consumer.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.as_ptr(), pointer);
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    drop(received);
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    drop(pins);
    tokio::time::timeout(std::time::Duration::from_secs(1), replies.closed())
        .await
        .unwrap();
}

#[tokio::test]
async fn closing_blocked_managed_route_releases_external_reply_once() {
    let engine = engine();
    let mut pins = PacketSessionPins::default().managed(engine.clone(), "tun".into());
    let bytes = packet();
    let drops = Arc::new(AtomicUsize::new(0));
    let (destination, mut consumer) = tokio::sync::mpsc::channel::<PacketBuffer>(1);
    destination.send(vec![0].into()).await.unwrap();
    let plane = PacketPlane::Packet("endpoint".into());
    let replies = pins
        .replies_for(&bytes, &plane, None, destination.into())
        .unwrap();
    pins.record(&bytes, plane);
    replies
        .send_buffer(PacketBuffer::from_owner(Owner(bytes, drops.clone())))
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(drops.load(Ordering::Relaxed), 0);
    let route = engine
        .packet_routes_snapshot(&zero_api::PacketRouteListQuery::default())
        .routes
        .remove(0);
    let (_, control) = engine
        .begin_close_packet_route(&zero_api::PacketRouteCloseCommand {
            route_id: route.route_id,
            expected_core_instance_id: engine.core_instance_id().into(),
            expected_config_revision: None,
        })
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), pins.management_changed())
        .await
        .unwrap();
    pins.expire();
    tokio::time::timeout(std::time::Duration::from_secs(1), control.wait_released())
        .await
        .unwrap();
    assert!(replies.is_closed());
    assert_eq!(drops.load(Ordering::Relaxed), 1);
    assert_eq!(consumer.recv().await.unwrap().as_ref(), &[0]);
    assert!(consumer.recv().await.is_none());
}
