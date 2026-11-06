//! Real original SDK task with no Administrator/TUN precondition.
use super::*;
use crate::packet_memory::PacketReturnTotals;
use std::{future::Future, task::Poll};
fn pending_request() -> OwnedWintunRequest {
    OwnedWintunRequest::Create {
        name: None,
        addresses: Vec::new(),
        mtu: 0,
    }
}
#[tokio::test]
async fn invalid_request_runs_on_the_original_reader_and_retains_its_typed_source_until_real_join()
{
    let root = Arc::new(PacketReturnTotals::default());
    let owner = OwnedWintunOwner::new();
    let work = owner
        .prepare_read_work(root.native_grant(65_536).unwrap())
        .unwrap();
    let control = owner.read_control().unwrap();
    let thread = std::thread::spawn(move || work.run());
    let actual_thread = thread.thread().id();
    let failure = owner
        .acquire_in_reader(pending_request())
        .await
        .err()
        .unwrap();
    assert_eq!(failure.kind(), io::ErrorKind::InvalidInput);
    let original = owner.acquisition_failure().unwrap();
    assert!(std::ptr::eq(
        failure
            .get_ref()
            .unwrap()
            .downcast_ref::<SharedSource>()
            .unwrap()
            .0
            .as_ref(),
        original.as_ref()
    ));
    assert!(owner.state().adapter.is_none());
    assert!(owner.state().session.is_none());
    assert!(owner.closed_receipt().is_none());
    assert_eq!(thread.thread().id(), actual_thread);
    let joined = thread.join().unwrap().unwrap_err();
    assert_eq!(joined.kind(), io::ErrorKind::InvalidInput);
    assert!(
        control.is_open(),
        "task join cannot close its original event"
    );
    assert!(owner.close().await.unwrap().is_none());
    assert!(!control.is_open());
    assert!(Arc::ptr_eq(
        &original,
        &owner.acquisition_failure().unwrap()
    ));
    assert!(owner.acquire_in_reader(pending_request()).await.is_err());
    assert_eq!(root.native().occupied.frames, 0);
}
#[tokio::test]
async fn cancelled_pending_acquisition_keeps_original_input_and_worker_until_same_join_and_close() {
    let root = Arc::new(PacketReturnTotals::default());
    let owner = OwnedWintunOwner::new();
    let work = owner
        .prepare_read_work(root.native_grant(65_536).unwrap())
        .unwrap();
    let control = owner.read_control().unwrap();
    let thread = std::thread::spawn(move || work.run());
    let pause = control.pause().await.unwrap();
    let mut opening = Box::pin(owner.acquire_in_reader(pending_request()));
    std::future::poll_fn(|cx| {
        assert!(opening.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(owner
        .state()
        .prepared_reader
        .as_ref()
        .unwrap()
        .acquisition
        .is_some());
    drop(opening);
    assert!(owner.closing.load(Ordering::Acquire));
    assert!(owner.closed_receipt().is_none());
    drop(pause);
    thread.join().unwrap().unwrap();
    assert!(owner
        .state()
        .prepared_reader
        .as_ref()
        .unwrap()
        .acquisition
        .is_none());
    assert!(owner.state().adapter.is_none());
    assert!(owner.state().session.is_none());
    assert!(control.is_open());
    assert!(owner.close().await.unwrap().is_none());
    assert!(!control.is_open());
    assert_eq!(root.native().occupied.frames, 0);
}
