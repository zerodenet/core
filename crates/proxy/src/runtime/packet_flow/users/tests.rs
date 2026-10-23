use super::*;
use std::{future::Future, task::Poll, time::Duration};
use zero_platform_tokio::contract::scope::{HostedScope, TaskClass, TaskScope};

fn limit() -> NonZeroUsize {
    NonZeroUsize::new(1).unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(2)
}
fn hosted() -> HostedScope {
    let mut scope = TaskScope::suspended_hosted(tokio::runtime::Handle::current(), Some(limit()));
    scope.enable_registration().unwrap();
    scope
}
async fn pending(future: impl Future) {
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        assert!(future.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
}
fn failed_user(result: Result<FlowUser, ChannelError>) -> ChannelError {
    match result {
        Ok(_) => panic!("unexpected user admission"),
        Err(error) => error,
    }
}
#[test]
fn user_owner_is_pure_and_an_unscoped_leaf_cannot_sign_users_closed() {
    assert!(tokio::runtime::Handle::try_current().is_err());
    let users = FlowUsers::new(limit());
    assert_eq!(users.slots.active(), 0);
    users.mark_unaccounted();
    assert!(users.state.lock().unwrap().unaccounted);
    let weak = Arc::downgrade(&users);
    drop(users);
    assert!(weak.upgrade().is_none());
}
#[tokio::test]
async fn view_drop_and_body_ready_do_not_refund_capacity_before_the_original_scope_join() {
    let users = FlowUsers::new(limit());
    let mut scope = hosted();
    let (ready, body_ready) = tokio::sync::oneshot::channel();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            ready.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    let user = users
        .acquire(Some(FlowCaller {
            witness: witness.clone(),
        }))
        .unwrap();
    body_ready.await.unwrap();
    drop(user);
    assert!(!witness.is_joined());
    assert_eq!(failed_user(users.acquire(None)).reason, Reason::Capacity);
    pending(users.shutdown(deadline())).await; // cancelling only this waiter
    assert_eq!(users.slots.active(), 1);
    let exit = scope.next_receipt().await.unwrap();
    assert!(witness.is_joined());
    let report = users.shutdown(deadline()).await.unwrap();
    assert!(report.is_complete());
    assert!(report.closed_io.is_empty());
    assert!(report.exited_tasks.is_empty()); // observation is not host delivery
    assert_eq!(users.slots.active(), 0);
    assert!(!scope.is_drained());
    scope.acknowledge(&exit).unwrap();
}
#[tokio::test]
async fn a_real_join_still_requires_the_transferred_view_to_be_released() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let users = FlowUsers::new(limit());
    let mut scope = hosted();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async { Ok(()) })
        .unwrap();
    let user = users.acquire(Some(FlowCaller { witness })).unwrap();
    let foreign = FlowUsers::new(limit());
    assert!(users.validates(&user));
    assert!(!foreign.validates(&user));
    let (io, mut peer) = tokio::io::duplex(32);
    let mut stream = TrackedStream::new(io, user);
    stream.write_all(b"real wire").await.unwrap();
    let mut received = [0; 9];
    peer.read_exact(&mut received).await.unwrap();
    assert_eq!(&received, b"real wire");
    let exit = scope.next_receipt().await.unwrap();
    pending(users.shutdown(deadline())).await;
    assert_eq!(users.slots.active(), 1);
    // Check one native trait poll first: a regression to permanently
    // Interrupted must fail before read helpers can spin inside one poll.
    let mut bytes = [0; 1];
    let mut buffer = tokio::io::ReadBuf::new(&mut bytes);
    let error = std::future::poll_fn(|cx| {
        tokio::io::AsyncRead::poll_read(std::pin::Pin::new(&mut stream), cx, &mut buffer)
    })
    .await
    .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::BrokenPipe);
    let read = tokio::time::timeout(Duration::from_secs(2), stream.read_u8())
        .await
        .expect("retired stream must terminate an AsyncReadExt operation")
        .unwrap_err();
    assert_eq!(read.kind(), std::io::ErrorKind::BrokenPipe);
    let write = stream.write_all(b"retired").await.unwrap_err();
    assert_eq!(write.kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(users.slots.active(), 1);
    drop(stream);
    assert!(users.shutdown(deadline()).await.unwrap().is_complete());
    assert_eq!(users.slots.active(), 0);
    scope.acknowledge(&exit).unwrap();
}
#[tokio::test]
async fn missing_original_caller_or_unaccounted_leaf_remains_unfinished_after_drop() {
    let users = FlowUsers::new(limit());
    drop(users.acquire(None).unwrap());
    assert!(!users.shutdown(deadline()).await.unwrap().is_complete());
    assert_eq!(users.slots.active(), 1);
    let users = FlowUsers::new(limit());
    users.mark_unaccounted();
    assert!(!users.shutdown(deadline()).await.unwrap().is_complete());
}
#[tokio::test]
async fn reaping_reuses_finite_records_and_preserves_original_failure_arc_and_decisions() {
    let users = FlowUsers::new(limit());
    let mut scope = hosted();
    for _ in 0..3 {
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async { Ok(()) })
            .unwrap();
        let user = users.acquire(Some(FlowCaller { witness })).unwrap();
        drop(user);
        let exit = scope.next_receipt().await.unwrap();
        users.reap();
        assert_eq!(users.slots.active(), 0);
        assert!(users.state.lock().unwrap().records.is_empty());
        scope.acknowledge(&exit).unwrap();
    }
    let mut original = ChannelError::new(Stage::Receive, Reason::Io, Impact::Host)
        .with_source(std::io::Error::from_raw_os_error(10013));
    original.component = Some(zero_core::component::ComponentId(19));
    original.session = Some(zero_core::component::SessionId(23));
    original.delivery = zero_core::channel::DeliveryState::LocallyAccepted;
    original.retry = zero_core::channel::RetryClass::Revalidate;
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async { Err(original) })
        .unwrap();
    drop(
        users
            .acquire(Some(FlowCaller {
                witness: witness.clone(),
            }))
            .unwrap(),
    );
    let exit = scope.next_receipt().await.unwrap();
    let original = exit.result().unwrap_err();
    for _ in 0..2 {
        let error = users.shutdown(deadline()).await.unwrap_err();
        assert_eq!(
            (
                error.stage,
                error.reason,
                error.impact,
                error.component,
                error.session,
                error.delivery,
                error.retry
            ),
            (
                original.stage,
                original.reason,
                original.impact,
                original.component,
                original.session,
                original.delivery,
                original.retry
            )
        );
        let retained = users.state.lock().unwrap().first.clone().unwrap();
        assert!(Arc::ptr_eq(&retained, &original));
        let mut source: &(dyn std::error::Error + 'static) = &error;
        let mut actual_os = None;
        while let Some(next) = source.source() {
            if let Some(io) = next.downcast_ref::<std::io::Error>() {
                actual_os = actual_os.or(io.raw_os_error());
            }
            source = next;
        }
        assert_eq!(actual_os, Some(10013));
    }
    assert!(!exit.local_failure_accepted()); // original ingress receiver owns ACK
    witness
        .acknowledge_hosted_failure(&witness.wait().await)
        .unwrap();
    scope.acknowledge(&exit).unwrap();
}
#[tokio::test]
async fn scope_drop_does_not_close_a_released_view_without_actual_join() {
    let users = FlowUsers::new(limit());
    let scope = hosted();
    let (_, witness) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, std::future::pending())
        .unwrap();
    drop(
        users
            .acquire(Some(FlowCaller {
                witness: witness.clone(),
            }))
            .unwrap(),
    );
    drop(scope);
    pending(users.shutdown(deadline())).await;
    assert!(!witness.is_joined());
    assert_eq!(users.slots.active(), 1);
}

mod pending;
