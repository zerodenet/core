//! Custody/queue tests use the actual Proxy Host books. No native-close claim.
use super::*;
use crate::runtime::Proxy;
use zero_config::RuntimeConfig;
use zero_core::{component::AssociationId, Address};
fn fixture() -> (Proxy, BusinessMemory) {
    let proxy =
        Proxy::new(RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap())
            .unwrap();
    let host = proxy.current_service_source().upgrade().unwrap();
    let (bytes, metadata) = host.packet_path_memory().unwrap();
    (proxy, BusinessMemory { bytes, metadata })
}
fn packet(memory: &BusinessMemory, index: usize) -> Packet {
    let target = Address::Domain("held.example".into());
    let original = memory
        .datagram(AssociationId(13), &target, 53, &index.to_be_bytes())
        .unwrap();
    (original.address.address, 53, original.payload, Some(29))
}
#[tokio::test]
async fn finite_packet_responses_keep_original_host_tickets_through_delivery_and_never_forge_native_joins(
) {
    let (_proxy, memory) = fixture();
    let baseline = memory.metadata.snapshot().occupied;
    let (owner, mut lease) = ResponseSetOwner::new(JoinSet::new());
    let sender = lease.packet_sender(&memory).unwrap();
    let maximum = memory.bytes.limits().max_queue_packets;
    for index in 0..maximum {
        sender.send(packet(&memory, index)).unwrap();
    }
    let charged = memory.bytes.used_bytes();
    assert!(charged > 0);
    assert!(sender.send(packet(&memory, maximum)).is_err());
    assert_eq!(
        memory.bytes.used_bytes(),
        charged,
        "rejected new output must not evict live responses"
    );
    assert!(
        lease.is_empty(),
        "data-only delivery starts no native tasks"
    );
    for index in 0..maximum {
        let actual = lease.join_next_with_id().await.unwrap().unwrap().unwrap();
        assert_eq!(actual.0, Address::Domain("held.example".into()));
        assert_eq!(actual.1, 53);
        assert_eq!(actual.2.as_ref(), &index.to_be_bytes());
        assert_eq!(actual.3, Some(29));
        assert!(
            memory.bytes.used_bytes() > 0,
            "dequeue does not refund the responder's live payload"
        );
        drop(actual);
    }
    assert_eq!(memory.bytes.used_bytes(), 0);
    assert_eq!(owner.evidence.statistics.lock().unwrap().joined, 0);
    sender.send(packet(&memory, 31)).unwrap();
    owner.revoke();
    assert_eq!(memory.bytes.used_bytes(), 0);
    assert!(sender.send(packet(&memory, 32)).is_err());
    assert!(lease.join_next_with_id().await.is_none());
    drop(lease);
    let closed = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(
        closed.joined_tasks(),
        0,
        "data drain is never a fabricated task receipt"
    );
    drop(sender);
    drop(owner);
    assert_eq!(memory.metadata.snapshot().occupied, baseline);
}
#[tokio::test]
async fn packet_inbox_rejects_foreign_host_and_real_native_panic_still_wins_before_ready_data() {
    let (_proxy, memory) = fixture();
    let (_foreign, foreign) = fixture();
    let (owner, mut lease) = ResponseSetOwner::new(JoinSet::new());
    let sender = lease.packet_sender(&memory).unwrap();
    assert!(lease.packet_sender(&foreign).is_err());
    sender.send(packet(&memory, 19)).unwrap();
    let (started, ready) = tokio::sync::oneshot::channel();
    let task = lease.spawn(async move {
        started.send(()).unwrap();
        panic!("actual original response panic");
    });
    ready.await.unwrap();
    while !task.is_finished() {
        tokio::task::yield_now().await;
    }
    let original = lease.join_next_with_id().await.unwrap().unwrap_err();
    assert!(original.is_panic());
    assert!(Arc::ptr_eq(
        &original,
        &owner.first_fatal_failure().unwrap()
    ));
    let packet = lease.join_next_with_id().await.unwrap().unwrap().unwrap();
    assert_eq!(packet.2.as_ref(), &19usize.to_be_bytes());
    drop(packet);
    drop(lease);
    let closed = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(2))
        .await
        .unwrap();
    assert_eq!(closed.joined_tasks(), 1);
    assert!(Arc::ptr_eq(
        &original,
        &owner.first_fatal_failure().unwrap()
    ));
}
