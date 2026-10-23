use super::*;
use std::{error::Error, io};
use zero_core::{
    channel::{DeliveryState, RetryClass},
    component::{ComponentId, SessionId},
};

#[tokio::test]
async fn tracked_failure_shares_original_arc_native_payload_and_every_decision_field() {
    let mut owner = scope(1);
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let native = Arc::new(std::net::TcpListener::bind(held.local_addr().unwrap()).unwrap_err());
    let native_weak = Arc::downgrade(&native);
    let raw = native.raw_os_error().expect("actual address-in-use source");
    let mut error =
        ChannelError::new(Stage::Handshake, Reason::Io, Impact::Host).with_source(native.clone());
    error.component = Some(ComponentId(17));
    error.session = Some(SessionId(23));
    error.delivery = DeliveryState::NotSubmitted;
    error.retry = RetryClass::Revalidate;
    drop(native);
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move { Err(error) })
        .unwrap();
    let exit = owner.next_exit().await.unwrap();
    let forwarded = exit.result.unwrap_err();
    let receipt = witness.wait().await;
    let original = receipt.result().unwrap_err();
    let other = witness.clone().wait().await.result().unwrap_err();
    assert!(Arc::ptr_eq(&original, &other));
    assert_eq!(receipt.id(), id);
    assert_eq!(receipt.class(), TaskClass::Critical);
    assert_eq!(forwarded.component, original.component);
    assert_eq!(forwarded.session, original.session);
    assert_eq!(forwarded.stage, original.stage);
    assert_eq!(forwarded.reason, original.reason);
    assert_eq!(forwarded.impact, original.impact);
    assert_eq!(forwarded.delivery, original.delivery);
    assert_eq!(forwarded.retry, original.retry);
    let forwarded_original = forwarded
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap();
    assert!(
        std::ptr::eq(forwarded_original, original.as_ref()),
        "source wrapper reaches the same original error Arc"
    );
    let native = original
        .source()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(native, &native_weak.upgrade().unwrap()));
    assert_eq!(native.raw_os_error(), Some(raw));
    let weak = Arc::downgrade(&witness.0);
    drop(other);
    drop(original);
    drop(receipt);
    drop(forwarded);
    drop(witness);
    assert!(weak.upgrade().is_none());
    assert!(
        native_weak.upgrade().is_none(),
        "no scope-held failure/witness history remains after consuming the event"
    );
    assert!(owner.0.state.lock().unwrap().records.is_empty());
}

#[tokio::test]
async fn panic_receipt_and_shutdown_report_retain_same_native_join_error_object() {
    let mut owner = scope(1);
    let (native_id, observed) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            native_id.send(tokio::task::id()).unwrap();
            panic!("tracked actual task panic");
        })
        .unwrap();
    let native_id = observed.await.unwrap();
    let report = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.failed_tasks.len(), 1);
    assert_eq!(report.failed_tasks[0].id, id);
    assert_eq!(report.failed_tasks[0].class, TaskClass::Auxiliary);
    let receipt = witness.wait().await;
    let original = receipt.result().unwrap_err();
    let native = original
        .source()
        .unwrap()
        .downcast_ref::<JoinError>()
        .unwrap();
    assert!(native.is_panic());
    assert_eq!(native.id(), native_id);
    let same = report.failed_tasks[0]
        .error
        .source()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<ChannelError>()
        .unwrap()
        .source()
        .unwrap()
        .downcast_ref::<JoinError>()
        .unwrap();
    assert!(std::ptr::eq(native, same));
    assert_eq!(original.reason, Reason::Internal);
}

#[tokio::test]
async fn ordinary_untracked_failure_keeps_original_direct_source_chain() {
    let mut owner = scope(1);
    let original = Arc::new(io::Error::other("existing direct source"));
    let retained = original.clone();
    owner
        .registrar()
        .spawn(TaskClass::Critical, async move {
            Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(original),
            )
        })
        .unwrap();
    let error = owner.next_exit().await.unwrap().result.unwrap_err();
    assert!(Arc::ptr_eq(
        error
            .source()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &retained
    ));
}

#[tokio::test]
async fn dropping_every_witness_before_join_keeps_the_unshared_original_failure() {
    let mut owner = scope(1);
    let original = Arc::new(io::Error::other("dropped observation"));
    let retained = original.clone();
    let (_, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            Err(
                ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                    .with_source(original),
            )
        })
        .unwrap();
    let weak = Arc::downgrade(&witness.0);
    drop(witness);
    assert!(weak.upgrade().is_none());
    let error = owner.next_exit().await.unwrap().result.unwrap_err();
    assert!(Arc::ptr_eq(
        error
            .source()
            .unwrap()
            .downcast_ref::<Arc<io::Error>>()
            .unwrap(),
        &retained
    ));
}
