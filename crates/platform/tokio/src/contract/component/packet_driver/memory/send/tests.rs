//! Concrete original memory tests, not native/caller/Scope qualification.
use super::*;
use std::mem::size_of;
use zero_traits::packet_metadata::{PacketMetadataCounter, PacketMetadataLayout};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
fn root(available: PacketMetadataLayout) -> Arc<PacketReturnTotals> {
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
                .checked_add(available)
                .unwrap(),
        )
        .unwrap(),
    )
}
#[test]
fn actual_codec_shape_table_pays_growth_peak_and_returns_only_after_real_array_drop() {
    let root = root(PacketMetadataLayout::new(2, 5 * size_of::<usize>()));
    let baseline = root.driver_metadata().occupied;
    let native = root.native();
    let mut table = prepare_lengths(&root.driver_metadata_view(), 3).unwrap();
    assert_eq!(table.capacity(), 3);
    table.copy_from_slice(&[11, 17, 29]);
    assert_eq!(&*table, &[11, 17, 29]);
    assert_eq!(
        root.driver_metadata().occupied,
        baseline
            .checked_add(PacketMetadataLayout::array::<usize>(3).unwrap())
            .unwrap()
    );
    assert_eq!(
        root.native(),
        native,
        "CPU shape dimensions do not mint Native frames"
    );
    drop(table);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn refused_codec_shape_table_never_enters_a_codec_or_changes_native_tickets() {
    let root = root(PacketMetadataLayout::new(1, 2 * size_of::<usize>()));
    let native = root.native();
    let baseline = root.driver_metadata().occupied;
    let error = match prepare_lengths(&root.driver_metadata_view(), 2) {
        Ok(_) => panic!("old+new shape arrays cannot fit this original Root"),
        Err(error) => error,
    };
    assert_eq!(error.reason, Reason::Capacity);
    assert_eq!(error.delivery, DeliveryState::NotSubmitted);
    assert_eq!(root.driver_metadata().occupied, baseline);
    assert_eq!(root.native(), native);
    let one = prepare_lengths(&root.driver_metadata_view(), 1).unwrap();
    assert_eq!(&*one, &[0]);
    drop(one);
    assert_eq!(root.driver_metadata().occupied, baseline);
}
#[test]
fn exact_datagram_wrapper_header_follows_same_payload_until_actual_erased_box_is_freed() {
    let root = Arc::new(PacketReturnTotals::default());
    let original = root.native_grant(32).unwrap();
    let read = original.try_buffer().unwrap();
    let payload = read
        .into_packet(4)
        .unwrap_or_else(|_| panic!("same read fits four bytes"));
    let before = root.driver_metadata().occupied;
    let layout = PacketMetadataLayout::boxed::<DatagramCopyStorage>();
    let header = root.driver_metadata_view().reserve(layout).unwrap();
    let wrapped = zero_traits::PacketBuffer::from_prepaid_owner(
        DatagramCopyStorage {
            payload,
            metadata: None,
        },
        header,
    )
    .unwrap_or_else(|_| panic!("same exact prepaid wrapper layout"));
    assert_eq!(wrapped.len(), 4);
    assert_eq!(
        root.driver_metadata().occupied,
        before.checked_add(layout).unwrap()
    );
    original.revoke();
    assert_eq!(
        root.native().occupied.frames,
        1,
        "actual payload still owns its read"
    );
    drop(wrapped);
    assert_eq!(root.native().occupied.frames, 0);
    drop(original);
    assert_eq!(
        root.driver_metadata().occupied,
        PacketMetadataCounter::owned_layout()
    );
}
