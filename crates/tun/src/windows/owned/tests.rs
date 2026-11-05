use super::*;
use std::{future::Future, task::Poll};
use tokio::io::AsyncReadExt;

#[test]
fn preflight_failure_retains_its_original_source_and_cannot_replay_acquisition() {
    let owner = OwnedWintunOwner::new();
    assert!(matches!(
        owner.outcome(),
        WintunAcquisitionOutcome::KnownNoNative
    ));
    let failure = owner.open_existing("", &[], 0).err().unwrap();
    let source = owner.acquisition_failure().unwrap();
    assert_eq!(failure.kind(), source.kind());
    assert!(matches!(
        owner.outcome(),
        WintunAcquisitionOutcome::KnownNoNative
    ));
    assert!(owner.open_existing("", &[], 0).is_err());
    assert!(Arc::ptr_eq(&source, &owner.acquisition_failure().unwrap()));
}

#[tokio::test]
#[ignore = "requires Administrator privileges and wintun.dll; creates a dedicated test adapter"]
async fn cancelled_close_retains_actual_reader_and_both_native_originals_until_same_matching_receipt(
) {
    let owner = OwnedWintunOwner::new();
    let mut view = owner.create(Some("ZeroOwnedReceiptTest")).unwrap();
    let identity = view.owned_identity().clone();
    let mut bytes = [0; 65536];
    let mut reading = Box::pin(view.read(&mut bytes));
    std::future::poll_fn(|cx| {
        assert!(reading.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let actual_thread = owner.state().reader.as_ref().unwrap().thread().id();
    let mut closing = Box::pin(owner.close());
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(closing);
    assert_eq!(
        owner.state().reader.as_ref().unwrap().thread().id(),
        actual_thread
    );
    assert!(owner.state().session.is_some());
    assert!(owner.state().adapter.is_some());
    assert!(owner.closed_receipt().is_none());
    drop(reading);
    drop(view);
    let receipt = tokio::time::timeout(std::time::Duration::from_secs(5), owner.close())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(receipt.identity().same_owner(&identity));
    assert!(owner.state().reader.is_none());
    assert!(matches!(owner.state().reader_joined, Some(Ok(()))));
    assert!(owner.state().session.is_none());
    assert!(owner.state().adapter.is_none());
    assert!(Arc::ptr_eq(
        &receipt,
        &owner.close().await.unwrap().unwrap()
    ));
    assert!(matches!(owner.outcome(), WintunAcquisitionOutcome::Closed));
}
