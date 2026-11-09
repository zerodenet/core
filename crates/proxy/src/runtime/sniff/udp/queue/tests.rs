//! Real metadata quota and original packet movement, without Native IO claims.
use super::*;
use std::sync::Arc;
use zero_core::{Address, InboundUdpDispatch, ProtocolType};
use zero_traits::packet_metadata::{PacketMetadataCounter, PacketMetadataLayout};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
fn packet(bytes: &[u8]) -> InboundUdpDispatch {
    InboundUdpDispatch::new(
        ProtocolType::UNKNOWN,
        Address::Ipv4([192, 0, 2, 1]),
        443,
        bytes.to_vec(),
        None,
    )
}
fn root() -> Arc<PacketReturnTotals> {
    let one = PacketMemoryLimit {
        frames: 1,
        bytes: 1,
    };
    Arc::new(
        PacketReturnTotals::new_with_metadata(
            one,
            one,
            one,
            PacketMetadataCounter::owned_layout()
                .checked_add(PacketMetadataLayout::array::<Queued<InboundUdpDispatch>>(1).unwrap())
                .unwrap(),
        )
        .unwrap(),
    )
}
#[test]
fn exact_queue_pressure_keeps_both_originals_in_order_without_second_ready_allocation() {
    let root = root();
    let native = root.native();
    let baseline = root.driver_metadata().occupied;
    let mut packets = Packets::new(Some(&root.driver_metadata_view()), 64);
    let first = packet(b"held first");
    let address = first.payload().as_ptr();
    assert!(packets.push(0, Some(0), first));
    assert!(packets.pop(0).is_none());
    let occupied = root.driver_metadata().occupied;
    let second = packet(b"same next");
    let next_address = second.payload().as_ptr();
    assert!(
        !packets.push(1, Some(1), second),
        "old+new table growth cannot fit"
    );
    assert_eq!(root.driver_metadata().occupied, occupied);
    assert!(
        packets.pop(0).is_none(),
        "new pressure does not retire an older classification"
    );
    assert!(
        packets.pop(1).is_none(),
        "the inline input also remains the exact Pending original"
    );
    packets.release(0, |original| original);
    packets.release(1, |original| original);
    let first = packets.pop(0).unwrap();
    let second = packets.pop(1).unwrap();
    assert_eq!(first.payload().as_ptr(), address);
    assert_eq!(second.payload().as_ptr(), next_address);
    assert_eq!(first.payload(), b"held first");
    assert_eq!(second.payload(), b"same next");
    assert_eq!(
        root.driver_metadata().occupied,
        occupied,
        "empty still-allocated array is not a refund"
    );
    assert_eq!(root.native(), native, "CPU rows cannot mint a Native read");
    drop(first);
    drop(second);
    drop(packets);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn tun_readiness_order_and_mux_original_sequence_use_same_single_row_custody() {
    let root = Arc::new(PacketReturnTotals::default());
    let mut packets = Packets::new(Some(&root.driver_metadata_view()), 64);
    assert!(packets.push(0, Some(7), 1u32));
    assert!(packets.push(1, None, 2u32));
    assert!(
        packets.pop(0).is_none(),
        "a MUX input cannot overtake its earlier Pending original"
    );
    assert_eq!(
        packets.pop_ready(),
        Some(2),
        "TUN keeps its existing Ready FIFO policy"
    );
    packets.release(7, |same| {
        assert_eq!(same, 1);
        same
    });
    assert_eq!(packets.pop_ready(), Some(1));
}

#[tokio::test]
async fn row_pressure_wait_is_really_pending_and_foreign_free_promotes_same_originals() {
    use std::future::Future;
    let one = PacketMemoryLimit {
        frames: 1,
        bytes: 1,
    };
    let row_bytes = std::mem::size_of::<Queued<InboundUdpDispatch>>();
    let root = Arc::new(
        PacketReturnTotals::new_with_metadata(
            one,
            one,
            one,
            PacketMetadataCounter::owned_layout()
                .checked_add(PacketMetadataLayout::new(2, 3 * row_bytes))
                .unwrap(),
        )
        .unwrap(),
    );
    struct Held {
        _bytes: Box<[u8]>,
        _credit: zero_traits::packet_metadata::PacketMetadataCredit,
    }
    let credit = root
        .driver_metadata_view()
        .reserve(PacketMetadataLayout::array::<u8>(2 * row_bytes).unwrap())
        .unwrap();
    let foreign = Held {
        _bytes: vec![0; 2 * row_bytes].into_boxed_slice(),
        _credit: credit,
    };
    let mut packets = Packets::new(Some(&root.driver_metadata_view()), 64);
    assert!(packets.push(0, Some(7), packet(b"first")));
    let second = packet(b"second");
    let pointer = second.payload().as_ptr();
    assert!(!packets.push(1, Some(7), second));
    {
        let work = packets
            .wait_capacity(tokio::time::Instant::now() + std::time::Duration::from_millis(200));
        tokio::pin!(work);
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(work.as_mut().poll(cx).is_pending()))
                .await
        );
    }
    assert!(packets.row_pressure());
    assert!(packets.pop(0).is_none());
    drop(foreign); // real CPU storage free before same-Root notification/refund
    packets
        .wait_capacity(tokio::time::Instant::now() + std::time::Duration::from_millis(200))
        .await;
    assert!(!packets.row_pressure());
    assert!(
        packets.pop(0).is_none(),
        "capacity promotion is not parser completion"
    );
    packets.release(7, |original| original);
    assert_eq!(packets.pop(0).unwrap().payload(), b"first");
    let original = packets.pop(1).unwrap();
    assert_eq!(original.payload().as_ptr(), pointer);
    assert_eq!(original.payload(), b"second");
}
