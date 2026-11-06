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

#[test]
fn failed_original_event_close_blocks_every_later_handle_use_without_claiming_closed() {
    let root = Arc::new(PacketReturnTotals::default());
    let control =
        OwnedWintunReadControl::new(root.native_grant(65_536).unwrap(), std::sync::Weak::new());
    assert!(control.activate().unwrap());
    let original = control.state().event.unwrap();
    // Deliberate real OS failure injection: invalidate this actual event, so
    // its controller cannot know whether its numeric handle was reused later.
    assert_ne!(unsafe { CloseHandle(original as HANDLE) }, 0);
    assert!(control.stop().is_err());
    assert!(control.signal_failure().is_some());
    let first = control.close().unwrap_err();
    let second = control.close().unwrap_err();
    assert!(Arc::ptr_eq(&first, &second));
    assert_eq!(control.state().event, Some(original));
    assert!(!control.state().closed);
    let replacement = unsafe { CreateEventW(std::ptr::null(), 0, 0, std::ptr::null()) };
    assert!(!replacement.is_null());
    assert!(control.signal().is_err());
    assert!(control.wait_handle().is_err());
    assert_eq!(
        unsafe { windows_sys::Win32::System::Threading::WaitForSingleObject(replacement, 0) },
        windows_sys::Win32::Foundation::WAIT_TIMEOUT
    );
    assert_ne!(unsafe { CloseHandle(replacement) }, 0);
    assert!(Arc::ptr_eq(
        &first,
        control.state().close_unknown.as_ref().unwrap()
    ));
}
