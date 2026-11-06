//! Actual original bootstrap/Win32 event and real reader join. This does not
//! create or qualify a TUN adapter/session and requires no Administrator token.
use super::*;
use crate::packet_memory::{PacketMemoryLimit, PacketReturnTotals};
use crate::{OwnedWintunOwner, WintunAcquisitionOutcome};
async fn waiting(owner: &OwnedWintunOwner) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            // The body sets this while owning the same mutex immediately before
            // Condvar::wait, and clears after reacquiring it. Observing it under
            // that mutex therefore identifies the real suspended input wait.
            if owner
                .state()
                .prepared_reader
                .as_ref()
                .unwrap()
                .waiting_session
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn reversible_pause_keeps_the_same_waiting_reader_and_event_until_actual_join_and_close() {
    let root = Arc::new(PacketReturnTotals::new(
        PacketMemoryLimit {
            frames: 1,
            bytes: 1,
        },
        PacketMemoryLimit {
            frames: 1,
            bytes: 65_536,
        },
    ));
    let grant = root.native_grant(65_536).unwrap();
    let owner = OwnedWintunOwner::new();
    let work = owner.prepare_read_work(grant).unwrap();
    let control = owner.read_control().unwrap();
    assert!(
        !control.is_open(),
        "preparation cannot acquire the event before actual task admission"
    );
    let thread = std::thread::spawn(move || work.run());
    waiting(&owner).await;
    let original_event = control.state().event.unwrap();
    assert!(matches!(owner.outcome(), WintunAcquisitionOutcome::Unknown));
    let pause = tokio::time::timeout(std::time::Duration::from_secs(2), control.pause())
        .await
        .unwrap()
        .unwrap();
    assert!(pause.payload_returned());
    assert!(!thread.is_finished());
    assert_eq!(control.state().event, Some(original_event));
    assert!(owner.closed_receipt().is_none());
    drop(pause);
    waiting(&owner).await;
    assert_eq!(control.state().event, Some(original_event));
    let second = tokio::time::timeout(std::time::Duration::from_secs(2), control.pause())
        .await
        .unwrap()
        .unwrap();
    assert!(!thread.is_finished());
    owner.stop();
    drop(second);
    thread.join().unwrap().unwrap();
    assert!(
        control.is_open(),
        "a body/real join does not close the native event"
    );
    assert!(
        owner.close().await.unwrap().is_none(),
        "no TUN was acquired; do not fabricate its receipt"
    );
    assert!(!control.is_open());
    assert!(control.state().closed);
    assert!(matches!(
        owner.outcome(),
        WintunAcquisitionOutcome::KnownNoNative
    ));
    assert_eq!(root.native().occupied.frames, 0);
}
