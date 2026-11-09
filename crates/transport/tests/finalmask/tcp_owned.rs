//! Same wire algorithms and real pending IO, with actual Host backing books.
use super::*;
use std::{num::NonZeroUsize, sync::Arc};
use zero_platform_tokio::{
    contract::component::PacketDriverMetadataView,
    controlled_carrier::{CarrierBufferBudget, CarrierBufferLimits, CarrierOriginalTotals},
};
use zero_tun::packet_memory::PacketReturnTotals;
#[path = "tcp_owned/pending.rs"]
mod pending;
#[path = "tcp_owned/wire.rs"]
mod wire;
fn roots() -> (CarrierBufferBudget, PacketDriverMetadataView) {
    let originals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    let packets = Arc::new(PacketReturnTotals::default());
    (originals.buffer_budget(), packets.driver_metadata_view())
}
fn custom() -> Mask {
    Mask::Custom(Custom {
        clients: vec![vec![Item {
            delay_ms: Range::default(),
            content: super::super::udp::Item::Bytes(b"client".to_vec()),
        }]],
        servers: vec![vec![Item {
            delay_ms: Range::default(),
            content: super::super::udp::Item::Bytes(b"server".to_vec()),
        }]],
        errors: Vec::new(),
    })
}
fn fragmented() -> Mask {
    Mask::Fragment(Fragment {
        packets: Range {
            minimum: 0,
            maximum: 1,
        },
        length: Range {
            minimum: 2,
            maximum: 2,
        },
        delay_ms: Range::default(),
        max_splits: Range {
            minimum: 2,
            maximum: 2,
        },
    })
}
#[tokio::test]
async fn material_rejection_refunds_same_books_before_mutable_mask_or_io_is_created() {
    let originals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    originals
        .install_buffer_limits_after_retirement(CarrierBufferLimits {
            max_queue_packets: 16,
            total_max_bytes: 1024,
        })
        .unwrap();
    let book = originals.buffer_budget();
    let packets = Arc::new(PacketReturnTotals::default());
    let metadata = packets.driver_metadata_view();
    let before = metadata.snapshot();
    let mask = Mask::Sudoku(super::super::sudoku::Settings {
        password: "secret".into(),
        ..Default::default()
    });
    assert!(PreparedMasks::new_owned(&[mask], book.clone(), metadata.clone()).is_err());
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(metadata.snapshot(), before);
    assert_eq!(originals.snapshot().charged, 0);
}
#[test]
fn streamed_identity_matches_original_debug_bytes_without_new_tables_or_wire_changes() {
    use crate::finalmask::{Profile, Settings};
    let settings = Settings {
        udp: vec![super::super::udp::Mask::Wireguard],
        tcp: vec![custom(), fragmented()],
    };
    let original = format!("{:?}:{:?}", settings.udp, settings.tcp);
    let expected: [u8; 32] = ring::digest::digest(&ring::digest::SHA256, original.as_bytes())
        .as_ref()
        .try_into()
        .unwrap();
    assert_eq!(Profile::new(settings).unwrap().identity(), expected);
}

#[tokio::test]
async fn accepted_raw_settings_survive_failed_build_and_same_book_recovery_without_unpaid_clone() {
    let originals = CarrierOriginalTotals::new(NonZeroUsize::new(1).unwrap());
    originals
        .install_buffer_limits_after_retirement(CarrierBufferLimits {
            max_queue_packets: 16,
            total_max_bytes: 1024,
        })
        .unwrap();
    let book = originals.buffer_budget();
    let packets = Arc::new(PacketReturnTotals::default());
    let metadata = packets.driver_metadata_view();
    let before = metadata.snapshot();
    let settings = crate::finalmask::Settings {
        udp: Vec::new(),
        tcp: vec![
            custom(),
            Mask::Sudoku(super::super::sudoku::Settings {
                password: "secret".into(),
                ..Default::default()
            }),
        ],
    };
    let expected = crate::finalmask::Profile::settings_identity(&settings);
    assert!(crate::finalmask::Profile::new_owned_from_ref(
        &settings,
        book.clone(),
        metadata.clone()
    )
    .is_err());
    assert_eq!(
        crate::finalmask::Profile::settings_identity(&settings),
        expected
    );
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(metadata.snapshot(), before);
    originals
        .install_buffer_limits_after_retirement(CarrierBufferLimits::default())
        .unwrap();
    let actual =
        crate::finalmask::Profile::new_owned_from_ref(&settings, book.clone(), metadata.clone())
            .unwrap();
    assert_eq!(actual.identity(), expected);
    assert!(actual.same_owned_budget(&book, &metadata));
    let (foreign, other_metadata) = roots();
    assert!(!actual.same_owned_budget(&foreign, &metadata));
    assert!(!actual.same_owned_budget(&book, &other_metadata));
    assert!(book.used_bytes() > 0);
    drop(actual);
    assert_eq!(book.used_bytes(), 0);
    assert_eq!(metadata.snapshot(), before);
    assert_eq!(
        crate::finalmask::Profile::settings_identity(&settings),
        expected
    );
}
