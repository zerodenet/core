//! Request accounting only: these tests mint no SDK/native closure evidence.
use super::*;
#[test]
fn default_is_one_shared_total_for_two_actual_max_capacity_requests() {
    let root = Arc::new(PacketReturnTotals::default());
    let first = root.reserve_requested_ring().unwrap();
    let second = root.reserve_requested_ring().unwrap();
    assert_eq!(first.capacity(), MAX_REQUEST);
    assert_eq!(second.capacity(), MAX_REQUEST);
    assert_eq!(
        root.requested_rings().requested.occupied,
        PacketMemoryLimit {
            frames: 2,
            bytes: 2 * MAX_REQUEST
        }
    );
    assert!(matches!(
        root.reserve_requested_ring(),
        Err(PacketMemoryError::Busy)
    ));
    first.release(); // explicit caller-owned accounting settlement; no native claim
    let next = root.reserve_requested_ring().unwrap();
    assert_eq!(root.requested_rings().requested.occupied.frames, 2);
    second.release();
    next.release();
    assert_eq!(root.requested_rings().requested.occupied.bytes, 0);
}
#[test]
fn unconfirmed_drop_cannot_advertise_requested_capacity_as_returned() {
    let root = Arc::new(PacketReturnTotals::default());
    let unknown = root.reserve_requested_ring().unwrap();
    drop(unknown);
    assert_eq!(
        root.requested_rings().requested.occupied,
        PacketMemoryLimit {
            frames: 1,
            bytes: MAX_REQUEST
        }
    );
    let independent = root.reserve_requested_ring().unwrap();
    assert!(matches!(
        root.reserve_requested_ring(),
        Err(PacketMemoryError::Busy)
    ));
    independent.release();
    assert_eq!(root.requested_rings().requested.occupied.bytes, MAX_REQUEST);
}
#[test]
fn busy_ring_shrink_changes_no_other_lane_and_old_request_returns_its_original_size() {
    let root = Arc::new(PacketReturnTotals::default());
    let first = root.reserve_requested_ring().unwrap();
    let second = root.reserve_requested_ring().unwrap();
    let old = [
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
    ];
    let old_meta = root.driver_metadata().limit;
    let next = PacketMemoryLimit {
        frames: 17,
        bytes: 1 << 20,
    };
    let ring = RequestedRingLimits {
        capacity: MAX_REQUEST / 2,
        total_requested_bytes: MAX_REQUEST,
    };
    assert_eq!(
        root.install_all_original_limits(next, next, next, old_meta, Some(ring)),
        Err(PacketMemoryError::Busy)
    );
    assert_eq!(
        [
            root.returns().limit,
            root.native().limit,
            root.transmit().limit
        ],
        old
    );
    assert_eq!(root.driver_metadata().limit, old_meta);
    assert_eq!(root.requested_rings().capacity, MAX_REQUEST);
    second.release();
    root.install_all_original_limits(next, next, next, old_meta, Some(ring))
        .unwrap();
    assert_eq!(root.requested_rings().requested.occupied.bytes, MAX_REQUEST);
    assert!(matches!(
        root.reserve_requested_ring(),
        Err(PacketMemoryError::Busy)
    ));
    first.release();
    let actual = root.reserve_requested_ring().unwrap();
    assert_eq!(actual.capacity(), MAX_REQUEST / 2);
    assert_eq!(
        root.requested_rings().requested.occupied.bytes,
        MAX_REQUEST / 2
    );
    actual.release();
}
