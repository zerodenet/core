use super::*;
use std::{num::NonZeroUsize, sync::Arc};
use zero_platform_tokio::contract::scope::{TaskClass, TaskRegistrationBudget, TaskScope};
use zero_tun::packet_memory::{PacketMemoryLimit, PacketMetadataLayout, PacketReturnTotals};
#[tokio::test]
async fn actual_udp_work_buffer_keeps_one_global_metadata_charge_without_changing_native_minimum_or_early_task_refund(
) {
    let root = Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 4,
            bytes: 1024,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 64,
        },
    ));
    let read = root.native_grant(64).unwrap();
    let view = read.memory_view().unwrap();
    let baseline = root.driver_metadata().occupied;
    let native = root.native();
    let mut buffer = UdpScratch::prepare(&view).unwrap();
    assert_eq!(buffer.len(), BYTES);
    buffer[BYTES - 1] = 17;
    assert_eq!(
        root.native(),
        native,
        "CPU scratch cannot steal the only native read frame"
    );
    assert_eq!(
        root.driver_metadata().occupied,
        baseline
            .checked_add(PacketMetadataLayout::array::<u8>(BYTES).unwrap())
            .unwrap()
    );
    let jobs = TaskRegistrationBudget::new(NonZeroUsize::new(1).unwrap());
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.bind_task_budget(jobs.clone()).unwrap();
    scope.enable_registration().unwrap();
    let (work, mut incoming) = tokio::sync::mpsc::channel::<tokio::sync::oneshot::Sender<u8>>(1);
    let (release, finished) = tokio::sync::oneshot::channel();
    let (_, actual) = scope
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            tokio::pin!(finished);
            loop {
                tokio::select! {
                    _=&mut finished=>break,
                    Some(reply)=incoming.recv()=>{reply.send(buffer[BYTES-1]).unwrap();},
                }
            }
            drop(buffer);
            Ok(())
        })
        .unwrap();
    for _ in 0..2 {
        let (reply, answered) = tokio::sync::oneshot::channel();
        work.send(reply).await.unwrap();
        assert_eq!(answered.await.unwrap(), 17);
        assert!(actual.try_joined().is_none());
        assert_eq!(jobs.usage().charged, 1);
        let occupied = root.driver_metadata().occupied;
        assert!(root
            .pause_driver_metadata_for_limit(PacketMetadataLayout::new(
                root.driver_metadata().limit.allocations(),
                occupied.bytes() - 1
            ))
            .is_err());
        assert_eq!(root.driver_metadata().occupied, occupied); // old worker stays live
    }
    scope.begin_shutdown();
    release.send(()).unwrap();
    let receipt = scope.next_receipt().await.unwrap();
    assert!(actual.matches_joined(receipt.joined()));
    assert_eq!(root.driver_metadata().occupied, baseline); // actual Box is gone
    assert_eq!(jobs.usage().charged, 1); // real task registration is a separate debt
    let defer = scope.defer_callbacks();
    scope.acknowledge(&receipt).unwrap();
    assert_eq!(jobs.usage().charged, 1);
    drop(defer);
    actual.wait_registration_released().await;
    assert_eq!(jobs.usage().charged, 0);
    assert_eq!(root.native(), native);
}
#[test]
fn a_new_udp_candidate_with_insufficient_working_bytes_is_rejected_without_charging_or_revoking_the_existing_root(
) {
    let root = Arc::new(PacketReturnTotals::default());
    let read = root.native_grant(64).unwrap();
    let view = read.memory_view().unwrap();
    let baseline = root.driver_metadata().occupied;
    root.install_original_limits(
        root.returns().limit,
        root.native().limit,
        root.transmit().limit,
        PacketMetadataLayout::new(64, baseline.bytes() + BYTES - 1),
    )
    .unwrap();
    assert!(UdpScratch::prepare(&view).is_err());
    assert_eq!(root.driver_metadata().occupied, baseline);
    view.check_open().unwrap();
    assert_eq!(root.native().occupied.frames, 1);
}
