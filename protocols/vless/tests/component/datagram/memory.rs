//! Real framing backing/storage Box ownership, independent of Native closure.
use super::*;
use std::num::NonZeroUsize;
use zero_core::{component::ComponentId, Address};
use zero_platform_tokio::{
    contract::admission::SessionOwner,
    controlled_carrier::{CarrierBufferLimits, CarrierOriginalTotals},
};
fn book(bytes: usize) -> CarrierBufferBudget {
    let root = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    root.install_buffer_limits_after_retirement(CarrierBufferLimits {
        max_queue_packets: 1,
        total_max_bytes: bytes,
    })
    .unwrap();
    root.buffer_budget()
}
#[tokio::test]
async fn actual_datagram_output_keeps_its_own_copy_and_header_after_all_framing_scratch_is_destroyed(
) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
        let metadata = root.driver_metadata_view();
        let baseline = metadata.snapshot().occupied;
        let book = book(1024 * 1024);
        let cancel = CancellationToken::new();
        let (_owner, admission) = SessionOwner::new(ComponentId(801), &cancel);
        let target = Destination::new(Address::Domain("actual.example".into()), 53);
        let budget = Budget::new(book.clone(), metadata.clone());
        let Prepared::Hosted(mut prepared) =
            Prepared::new(Some(&budget), &cancel, &admission, &target).unwrap()
        else {
            panic!("actual budget required")
        };
        prepared.tx.extend_from_slice(b"prepaid u16 framing");
        prepared
            .rx
            .extend_from_slice(b"separate original RX backing");
        let payload = b"independent received packet";
        let (payload, address) =
            std::future::poll_fn(|cx| prepared.output.poll_copy(cx, payload, &target))
                .await
                .unwrap();
        let frame = zero_core::packet::Datagram {
            association: zero_core::component::AssociationId(1),
            address,
            payload,
        };
        let in_flight = book.used_bytes();
        assert!(in_flight >= 3 * 65535);
        assert_eq!(frame.payload.as_ref(), b"independent received packet");
        drop(prepared);
        drop(budget);
        assert!(
            book.used_bytes() > 0 && book.used_bytes() < in_flight,
            "output copy is not paid by the destroyed scratch or Tx"
        );
        assert_ne!(
            metadata.snapshot().occupied,
            baseline,
            "its actual PacketStorage Box is still allocated"
        );
        drop(frame);
        assert_eq!(book.used_bytes(), 0);
        assert_eq!(metadata.snapshot().occupied, baseline);
    })
    .await
    .unwrap();
}
#[test]
fn insufficient_actual_datagram_working_peak_refuses_before_allocating_any_framing_backing() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let metadata = root.driver_metadata_view();
    let baseline = metadata.snapshot().occupied;
    let book = book(128 * 1024);
    let cancel = CancellationToken::new();
    let (_owner, admission) = SessionOwner::new(ComponentId(802), &cancel);
    let target = Destination::new(Address::Ipv4([127, 0, 0, 1]), 53);
    let budget = Budget::new(book.clone(), metadata.clone());
    let result = Prepared::new(Some(&budget), &cancel, &admission, &target);
    assert_eq!(result.err().unwrap().reason, Reason::Capacity);
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(metadata.snapshot().occupied, baseline);
}
#[tokio::test]
async fn empty_actual_packet_is_not_eof_and_next_output_uses_separate_root_credit() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let metadata = root.driver_metadata_view();
    let baseline = metadata.snapshot().occupied;
    let book = book(1024 * 1024);
    let cancel = CancellationToken::new();
    let (_owner, admission) = SessionOwner::new(ComponentId(803), &cancel);
    let target = Destination::new(Address::Ipv4([127, 0, 0, 1]), 53);
    let budget = Budget::new(book.clone(), metadata.clone());
    let Prepared::Hosted(mut prepared) =
        Prepared::new(Some(&budget), &cancel, &admission, &target).unwrap()
    else {
        panic!("actual budget required")
    };
    let first = std::future::poll_fn(|cx| prepared.output.poll_copy(cx, b"", &target))
        .await
        .unwrap();
    assert!(first.0.is_empty());
    let second = std::future::poll_fn(|cx| prepared.output.poll_copy(cx, b"next packet", &target))
        .await
        .unwrap();
    assert_eq!(second.0.as_ref(), b"next packet");
    drop(prepared);
    drop(first);
    assert!(book.used_bytes() > 0);
    drop(second);
    drop(budget);
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(metadata.snapshot().occupied, baseline);
}

#[tokio::test]
async fn original_request_uses_the_actual_prepaid_tx_allocation_before_native_open() {
    let root = Arc::new(zero_tun::packet_memory::PacketReturnTotals::default());
    let metadata = root.driver_metadata_view();
    let baseline = metadata.snapshot().occupied;
    let book = book(1024 * 1024);
    let cancel = CancellationToken::new();
    let (_owner, admission) = SessionOwner::new(ComponentId(804), &cancel);
    let target = Destination::new(Address::Domain("actual.example".into()), 53);
    let budget = Budget::new(book.clone(), metadata.clone());
    let mut prepared = Prepared::new(Some(&budget), &cancel, &admission, &target).unwrap();
    let Prepared::Hosted(hosted) = &prepared else {
        panic!("actual Host budget required")
    };
    let allocation = hosted.tx.as_ptr();
    let occupied = book.used_bytes();
    let headers = metadata.snapshot().occupied;
    let uuid = [7; 16];
    let expected = crate::shared::build_request_to(
        &target.address,
        target.port,
        &uuid,
        crate::shared::CMD_UDP,
    )
    .unwrap();
    let request = prepared.request(&target, &uuid).unwrap();
    assert_eq!(request.as_ptr(), allocation);
    assert_eq!(&*request, expected);
    assert_eq!(book.used_bytes(), occupied);
    assert_eq!(metadata.snapshot().occupied, headers);
    drop(request);
    drop(prepared);
    drop(budget);
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(metadata.snapshot().occupied, baseline);
}
