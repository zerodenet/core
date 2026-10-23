use super::*;
use std::{error::Error, io};
use zero_core::{
    channel::{DeliveryState, RetryClass},
    component::{ComponentId, SessionId},
};

#[tokio::test]
async fn hosted_tracked_failure_and_witness_share_original_arc_and_all_seven_fields() {
    let mut owner = scope(1);
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let native = Arc::new(std::net::TcpListener::bind(held.local_addr().unwrap()).unwrap_err());
    let native_weak = Arc::downgrade(&native);
    let raw = native.raw_os_error().unwrap();
    let mut error =
        ChannelError::new(Stage::Handshake, Reason::Io, Impact::Host).with_source(native.clone());
    error.component = Some(ComponentId(19));
    error.session = Some(SessionId(29));
    error.delivery = DeliveryState::NotSubmitted;
    error.retry = RetryClass::Revalidate;
    drop(native);
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move { Err(error) })
        .unwrap();
    let receipt = next(&mut owner).await;
    let original = receipt.result().unwrap_err();
    let weak = Arc::downgrade(&original);
    let witnessed = witness.wait().await.result().unwrap_err();
    assert!(Arc::ptr_eq(&original, &witnessed));
    assert_eq!(receipt.id(), id);
    assert_eq!(receipt.class(), TaskClass::Auxiliary);
    assert_eq!(original.component, Some(ComponentId(19)));
    assert_eq!(original.session, Some(SessionId(29)));
    assert_eq!(original.stage, Stage::Handshake);
    assert_eq!(original.reason, Reason::Io);
    assert_eq!(original.impact, Impact::Host);
    assert_eq!(original.delivery, DeliveryState::NotSubmitted);
    assert_eq!(original.retry, RetryClass::Revalidate);
    let native = original
        .source()
        .unwrap()
        .downcast_ref::<Arc<io::Error>>()
        .unwrap();
    assert!(Arc::ptr_eq(native, &native_weak.upgrade().unwrap()));
    assert_eq!(native.raw_os_error(), Some(raw));
    drop(receipt);
    drop(witness);
    drop(witnessed);
    drop(original);
    assert!(
        weak.upgrade().is_some(),
        "queue still owns the unacknowledged failure"
    );
    let retry = next(&mut owner).await;
    assert!(Arc::ptr_eq(
        &retry.result().unwrap_err(),
        &weak.upgrade().unwrap()
    ));
    owner.acknowledge(&retry).unwrap();
    assert!(
        weak.upgrade().is_some(),
        "receipt still observes the original outcome"
    );
    drop(retry);
    assert!(weak.upgrade().is_none());
    assert!(native_weak.upgrade().is_none());
    owner.begin_shutdown();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn hosted_panic_preserves_the_real_native_join_identity_and_same_source_on_redelivery() {
    let mut owner = scope(1);
    let (send, receive) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            send.send(tokio::task::id()).unwrap();
            panic!("real hosted driver panic");
        })
        .unwrap();
    let native_id = receive.await.unwrap();
    let receipt = next(&mut owner).await;
    let original = receipt.result().unwrap_err();
    let native = original
        .source()
        .unwrap()
        .downcast_ref::<JoinError>()
        .unwrap();
    assert!(native.is_panic());
    assert_eq!(native.id(), native_id);
    assert_eq!(receipt.id(), id);
    assert_eq!(receipt.class(), TaskClass::Critical);
    assert!(Arc::ptr_eq(
        &original,
        &witness.wait().await.result().unwrap_err()
    ));
    drop(receipt);
    let retry = next(&mut owner).await;
    let source = retry.result().unwrap_err();
    assert!(Arc::ptr_eq(&original, &source));
    assert!(std::ptr::eq(
        native,
        source
            .source()
            .unwrap()
            .downcast_ref::<JoinError>()
            .unwrap()
    ));
    owner.acknowledge(&retry).unwrap();
    owner.begin_shutdown();
    assert!(owner.is_drained());
}

#[test]
fn native_runtime_cancellation_is_joined_as_a_real_cancelled_exit_not_a_drop_receipt() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut owner = TaskScope::suspended_hosted(runtime.handle().clone(), NonZeroUsize::new(1));
    owner.enable_registration().unwrap();
    let (send, receive) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            send.send(tokio::task::id()).unwrap();
            std::future::pending::<Result<(), ComponentError>>().await
        })
        .unwrap();
    let native_id = runtime.block_on(receive).unwrap();
    pending(&mut owner);
    assert!(!witness.is_joined());
    drop(runtime); // Actual Tokio runtime teardown cancels its native task.
    assert!(
        !witness.is_joined(),
        "teardown alone does not perform a scope join"
    );
    let receipt = match owner.poll_exit(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(Some(receipt)) => receipt,
        other => panic!("actual task must have been cancelled and joined: {other:?}"),
    };
    assert_eq!(receipt.id(), id);
    let original = receipt.result().unwrap_err();
    let source = original
        .source()
        .unwrap()
        .downcast_ref::<JoinError>()
        .unwrap();
    assert!(source.is_cancelled());
    assert_eq!(source.id(), native_id);
    assert!(Arc::ptr_eq(
        &original,
        &witness.try_joined().unwrap().result().unwrap_err()
    ));
    owner.begin_shutdown();
    assert!(!owner.is_drained());
    owner.acknowledge(&receipt).unwrap();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn same_scope_wrong_offer_lease_cannot_acknowledge_the_real_entry() {
    let mut owner = scope(1);
    owner
        .registrar()
        .spawn(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let receipt = next(&mut owner).await;
    let forged = HostedExitReceipt {
        owner: receipt.owner.clone(),
        joined: receipt.joined.clone(),
        offer: Some(Arc::new(())),
    };
    assert_eq!(
        owner.acknowledge(&forged),
        Err(HostedReceiptError::StaleReceipt)
    );
    assert_eq!(counts(&owner), (0, 1, 0));
    owner.acknowledge(&receipt).unwrap();
    assert_eq!(counts(&owner), (0, 0, 0));
    owner.begin_shutdown();
    assert!(owner.is_drained());
}

#[tokio::test]
async fn explicit_failure_delivery_can_repeat_without_retaining_lifetime_source_history() {
    let mut owner = scope(1);
    for _ in 0..32 {
        owner
            .registrar()
            .spawn(TaskClass::Auxiliary, async {
                Err(
                    ChannelError::new(Stage::Receive, Reason::Io, Impact::Session)
                        .with_source(io::Error::other("local failure")),
                )
            })
            .unwrap();
        let receipt = next(&mut owner).await;
        let original = receipt.result().unwrap_err();
        let weak = Arc::downgrade(&original);
        assert_eq!(original.reason, Reason::Io);
        owner.acknowledge(&receipt).unwrap(); // This test is the explicit receiver.
        drop(original);
        drop(receipt);
        assert!(weak.upgrade().is_none());
        assert_eq!(counts(&owner), (0, 0, 0));
    }
    owner.begin_shutdown();
    assert!(owner.is_drained());
}
