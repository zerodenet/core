use super::*;
use std::sync::Arc;
use zero_tun::packet_memory::{NativeReadGrant, PacketMemoryLimit, PacketReturnTotals};
const CAPACITY: usize = u16::MAX as usize + 40;
fn read(grant: &NativeReadGrant, data: &[u8]) -> PacketBuffer {
    let mut read = grant
        .try_buffer()
        .expect("same original Native reader remains live");
    read.as_mut_bytes()[..data.len()].copy_from_slice(data);
    read.into_packet(data.len())
        .unwrap_or_else(|_| panic!("same native allocation"))
}
fn root() -> Arc<PacketReturnTotals> {
    Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
        PacketMemoryLimit {
            frames: 2,
            bytes: 2 * CAPACITY,
        },
    ))
}
#[tokio::test]
async fn admitted_fragment_survives_real_metadata_full_and_same_root_can_finish_after_refund() {
    for (source, target) in [("10.0.0.1", "10.0.0.2"), ("fd00::1", "fd00::2")] {
        let root = root();
        let baseline = root.driver_metadata().occupied;
        let grant = root.native_grant(CAPACITY).unwrap();
        let view = grant.memory_view().unwrap();
        let mut cache = OwnedFragmentReassembler::<u64>::new_with_memory(view.clone()).unwrap();
        let original = crate::packet::build_udp(
            source.parse().unwrap(),
            target.parse().unwrap(),
            1234,
            4321,
            &[9; 2048],
        );
        let fragments = crate::packet::fragment_ip_packet(&original, 576, 47);
        assert!(fragments.len() > 2);
        let now = Instant::now();
        assert!(matches!(
            cache.process(7, read(&grant, &fragments[0]), now).await,
            OwnedFragmentOutcome::Pending
        ));
        let received = cache.buffered_bytes();
        let admitted = root.driver_metadata().occupied;
        assert_eq!(
            root.native().occupied.frames,
            2,
            "one original reader plus one actual assembly"
        );
        let policy = root.driver_metadata().limit;
        root.install_original_limits(
            root.returns().limit,
            root.native().limit,
            root.transmit().limit,
            admitted,
        )
        .unwrap();
        assert!(matches!(
            cache.process(7, read(&grant, &fragments[1]), now).await,
            OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit)
        ));
        assert_eq!(cache.pending_datagrams(), 1);
        assert_eq!(
            cache.buffered_bytes(),
            received,
            "refused range cannot mutate old admitted payload/final state"
        );
        assert_eq!(root.driver_metadata().occupied, admitted);
        assert_eq!(root.native().occupied.frames, 2);
        assert!(grant.workspace_returned());
        root.install_original_limits(
            root.returns().limit,
            root.native().limit,
            root.transmit().limit,
            policy,
        )
        .unwrap();
        let mut completed = None;
        for fragment in &fragments[1..] {
            match cache.process(7, read(&grant, fragment), now).await {
                OwnedFragmentOutcome::Pending => {}
                OwnedFragmentOutcome::Packet {
                    packet,
                    reassembled: true,
                } => completed = Some(packet),
                _ => {
                    panic!("same admitted assembly finishes after real original policy is restored")
                }
            }
        }
        let packet = completed.unwrap();
        assert_eq!(
            crate::packet::parse_udp(&packet).unwrap().payload,
            &[9; 2048]
        );
        assert_eq!(packet.allocation_bytes(), CAPACITY);
        assert_eq!(cache.pending_datagrams(), 0);
        drop(cache);
        grant.revoke();
        drop(view);
        drop(grant);
        assert_eq!(root.native().occupied.frames, 1);
        assert!(
            root.driver_metadata().occupied.bytes() > baseline.bytes(),
            "actual result storage/header still alive"
        );
        drop(packet);
        assert_eq!(root.native().occupied.frames, 0);
        assert_eq!(root.driver_metadata().occupied, baseline);
    }
}
#[tokio::test]
async fn generation_reset_frees_old_arrays_and_preserves_the_same_source_metadata_binding() {
    let root = root();
    let grant = root.native_grant(CAPACITY).unwrap();
    let mut cache =
        OwnedFragmentReassembler::new_with_memory(grant.memory_view().unwrap()).unwrap();
    let cold = root.driver_metadata().occupied;
    let original = crate::packet::build_udp(
        "10.0.0.1".parse().unwrap(),
        "10.0.0.2".parse().unwrap(),
        1234,
        4321,
        &[7; 2048],
    );
    let fragments = crate::packet::fragment_ip_packet(&original, 576, 51);
    assert!(matches!(
        cache
            .process((), read(&grant, &fragments[0]), Instant::now())
            .await,
        OwnedFragmentOutcome::Pending
    ));
    assert_eq!(root.native().occupied.frames, 2);
    cache.reset();
    assert_eq!(root.native().occupied.frames, 1);
    assert_eq!(root.driver_metadata().occupied, cold);
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        cold,
    )
    .unwrap();
    assert!(matches!(
        cache
            .process((), read(&grant, &fragments[0]), Instant::now())
            .await,
        OwnedFragmentOutcome::Rejected(FragmentRejectReason::ResourceLimit)
    ));
    assert_eq!(
        cache.pending_datagrams(),
        0,
        "reset cannot silently switch Kernel cache to unmetered library construction"
    );
    assert_eq!(root.native().occupied.frames, 1);
    assert_eq!(root.driver_metadata().occupied, cold);
    grant.revoke();
}

#[tokio::test]
async fn actual_reassembled_native_reply_moves_to_client_queue_without_a_second_fragment_cache_or_payload_copy(
) {
    use crate::client_udp::{ClientUdpPacketEvent, ClientUdpStack};
    use crate::packet_output::{memory::PacketWireMemory, PacketSender};
    let root = root();
    let grant = root.native_grant(CAPACITY).unwrap();
    let view = grant.memory_view().unwrap();
    let (tx, mut output) = tokio::sync::mpsc::channel::<PacketBuffer>(1);
    let sender = PacketSender::from(tx)
        .with_packet_memory_view(view.clone(), PacketWireMemory::Native)
        .unwrap();
    let mut cache = OwnedFragmentReassembler::new_with_memory(view).unwrap();
    let local = "10.0.0.2".parse().unwrap();
    let remote = crate::packet::Endpoint {
        ip: "203.0.113.9".parse().unwrap(),
        port: 53,
    };
    let stack = ClientUdpStack::new_with_queue(vec![local], sender, 1500, None, Default::default())
        .unwrap();
    let mut socket = stack.bind(local).unwrap();
    socket.send_to(b"query", remote).await.unwrap();
    drop(output.recv().await.unwrap());
    let reply = crate::packet::build_udp(
        remote.ip,
        local,
        remote.port,
        socket.local_endpoint().port,
        &[3; 2048],
    );
    let fragments = crate::packet::fragment_ip_packet(&reply, 576, 73);
    let mut complete = None;
    for fragment in fragments {
        match cache
            .process((), read(&grant, &fragment), Instant::now())
            .await
        {
            OwnedFragmentOutcome::Pending => {}
            OwnedFragmentOutcome::Packet {
                packet,
                reassembled: true,
            } => complete = Some(packet),
            _ => panic!("same original/native reassembly must succeed"),
        }
    }
    let complete = complete.unwrap();
    assert!(
        crate::packet::parse_ip_fragment(&complete).is_none(),
        "the actual WG/Direct producer normalizes before ClientUdp"
    );
    let pointer = crate::packet::parse_udp(&complete)
        .unwrap()
        .payload
        .as_ptr();
    assert_eq!(root.native().occupied.frames, 2);
    stack.feed_correlated_packet(complete).unwrap();
    let event = std::future::poll_fn(|cx| socket.poll_recv_packet_event(cx))
        .await
        .unwrap();
    let ClientUdpPacketEvent::Datagram(packet) = event else {
        panic!("actual reply");
    };
    assert_eq!(packet.payload.as_ptr(), pointer);
    assert_eq!(packet.payload.as_ref(), &[3; 2048]);
    assert_eq!(packet.payload.allocation_bytes(), CAPACITY);
    assert_eq!(root.native().occupied.frames, 2);
    assert!(
        grant.try_buffer().is_ok(),
        "full Native2 still preserves the original reader promise"
    );
    let closed = socket.close();
    assert!(closed.io_quiescent());
    assert_eq!(
        root.native().occupied.frames,
        2,
        "logical/queue close cannot refund the actual receiver's payload"
    );
    drop(packet);
    assert_eq!(root.native().occupied.frames, 1);
    grant.revoke();
}
