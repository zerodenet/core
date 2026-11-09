use super::*;
use alloc::sync::Arc;
use core::task::Waker;
use zero_traits::packet_metadata::PacketMetadataCounter;

fn counter() -> Arc<PacketMetadataCounter> {
    PacketMetadataCounter::new_owned(PacketMetadataLayout::new(16, 8192)).unwrap()
}
fn packet() -> InboundUdpDispatch {
    InboundUdpDispatch::new(
        ProtocolType::UNKNOWN,
        Address::Ipv4([127, 0, 0, 1]),
        443,
        alloc::vec![1, 2, 3],
        None,
    )
}
#[test]
fn prepaid_name_keeps_actual_backing_credit_and_rejects_foreign_counter_rebinding() {
    let root = counter();
    let baseline = root.snapshot().occupied;
    let original = packet();
    let name = "actual.source.test";
    let layout = original
        .sniffed_domain_metadata_layout(name.len(), false)
        .unwrap();
    let paid = root.reserve(layout, Waker::noop().clone()).unwrap();
    let packet = original
        .with_sniffed_domain(name.into(), TargetHostSource::QuicSni, false)
        .with_domain_metadata(paid)
        .unwrap_or_else(|_| panic!("same prepaid name capacity"));
    assert_eq!(
        root.snapshot().occupied,
        baseline.checked_add(layout).unwrap()
    );
    let foreign = counter();
    let foreign_baseline = foreign.snapshot().occupied;
    let attempted = foreign.reserve(layout, Waker::noop().clone()).unwrap();
    let (source, packet, attempted) = packet.with_domain_metadata(attempted).err().unwrap();
    assert_eq!(source, PacketMemoryError::Stopped);
    assert_eq!(packet.target(), &Address::Domain(name.into()));
    assert!(packet.domain_metadata().unwrap().same_counter(
        &root
            .reserve(PacketMetadataLayout::new(0, 0), Waker::noop().clone())
            .unwrap()
    ));
    drop(packet);
    assert_eq!(root.snapshot().occupied, baseline);
    assert_ne!(foreign.snapshot().occupied, foreign_baseline);
    drop(attempted);
    assert_eq!(foreign.snapshot().occupied, foreign_baseline);
}
#[test]
fn original_domain_clone_and_route_only_dimensions_are_paid_before_transformation() {
    for route_only in [false, true] {
        let root = counter();
        let baseline = root.snapshot().occupied;
        let packet = packet().with_route_target(Address::Domain("route.source.test".into()));
        let name = "new.source.test";
        let layout = packet
            .sniffed_domain_metadata_layout(name.len(), route_only)
            .unwrap();
        let paid = root.reserve(layout, Waker::noop().clone()).unwrap();
        let packet = packet
            .with_sniffed_domain(name.into(), TargetHostSource::QuicSni, route_only)
            .with_domain_metadata(paid)
            .unwrap_or_else(|_| panic!("same exact future capacities"));
        assert_eq!(
            packet.route_target().unwrap(),
            &Address::Domain(
                if route_only {
                    name
                } else {
                    "route.source.test"
                }
                .into()
            )
        );
        assert_eq!(
            root.snapshot().occupied,
            baseline.checked_add(layout).unwrap()
        );
        drop(packet);
        assert_eq!(root.snapshot().occupied, baseline);
    }
}
