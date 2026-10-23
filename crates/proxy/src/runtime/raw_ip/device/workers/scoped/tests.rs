use super::*;
use std::{error::Error as _, future::Future, num::NonZeroUsize, task::Poll};
use zero_platform_tokio::contract::scope::{HostedScope, TaskScope};

fn hosted(limit: usize) -> HostedScope {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(limit));
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
#[tokio::test]
async fn body_ready_and_cancelled_join_waiter_never_replace_the_real_host_join() {
    let mut scope = hosted(1);
    let workers = ScopedWorkers::new(scope.registrar().downgrade());
    let (done, ready) = tokio::sync::oneshot::channel();
    workers
        .spawn(async move {
            done.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    ready.await.unwrap();
    let (_, witness) = workers.state.lock().unwrap().records[0]
        .registered()
        .unwrap();
    assert!(!witness.is_joined());
    pending(workers.join()).await; // drops only this wait
    assert!(!workers.is_drained());
    assert!(!witness.is_joined());
    let exit = scope.next_receipt().await.unwrap();
    assert_eq!(exit.class(), TaskClass::Critical);
    let joined = witness.wait().await;
    assert_eq!(joined.id(), exit.id());
    assert!(joined.result().is_ok());
    let report = workers.join().await.unwrap();
    assert_eq!(report.exited_tasks, vec![exit.id()]);
    assert!(workers.is_drained());
    assert!(report.closed_io.is_empty());
    // Local join observation does not ACK the host's own receipt or refund it.
    assert!(!scope.is_drained());
    scope.acknowledge(&exit).unwrap();
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    assert!(scope.is_drained());
}
#[tokio::test]
async fn real_panic_keeps_the_same_original_task_source_and_local_acceptance() {
    let mut scope = hosted(1);
    let workers = ScopedWorkers::new(scope.registrar().downgrade());
    let (done, ready) = tokio::sync::oneshot::channel();
    workers
        .spawn(async move {
            done.send(tokio::task::id()).unwrap();
            panic!("original scoped raw-IP worker panic")
        })
        .unwrap();
    let native_id = ready.await.unwrap();
    let exit = scope.next_receipt().await.unwrap();
    let original = exit.result().unwrap_err();
    assert!(!exit.local_failure_accepted());
    let error = workers.join().await.unwrap_err();
    assert!(workers.is_drained());
    assert!(exit.local_failure_accepted());
    let Failure::Task(retained) = workers.state.lock().unwrap().first.clone().unwrap() else {
        panic!("actual task source")
    };
    assert!(Arc::ptr_eq(&retained.result().unwrap_err(), &original));
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailure>()
        .unwrap();
    assert!(Arc::ptr_eq(&shared.0, &original));
    let mut source: &(dyn std::error::Error + 'static) = &error;
    let mut actual_panic = None;
    while let Some(next) = source.source() {
        if let Some(join) = next.downcast_ref::<tokio::task::JoinError>() {
            actual_panic = Some(join);
        }
        source = next;
    }
    let actual_panic = actual_panic.unwrap();
    assert!(actual_panic.is_panic());
    assert_eq!(actual_panic.id(), native_id);
    scope.acknowledge(&exit).unwrap();
}
#[tokio::test]
async fn actual_capacity_rejection_preserves_spawn_error_and_no_local_fallback() {
    let mut scope = hosted(1);
    let workers = ScopedWorkers::new(scope.registrar().downgrade());
    workers.spawn(std::future::pending()).unwrap();
    let error = workers.spawn(async { Ok(()) }).unwrap_err();
    assert_eq!(error.reason, Reason::Capacity);
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailure>()
        .unwrap();
    assert_eq!(
        shared.0.source().unwrap().downcast_ref::<SpawnError>(),
        Some(&SpawnError::Capacity)
    );
    assert_eq!(workers.state.lock().unwrap().records.len(), 1);
    let exit = scope.next_receipt().await.unwrap(); // original cancellation exits Ok
    assert!(exit.result().is_ok());
    assert!(workers.join().await.is_err()); // rejection remains the first failure
    assert!(workers.is_drained());
    scope.acknowledge(&exit).unwrap();
    assert!(workers.state.lock().unwrap().records.is_empty());
}
#[tokio::test]
async fn joined_records_are_reclaimed_and_weak_owner_drop_never_completes_a_witness() {
    let mut scope = hosted(1);
    let workers = ScopedWorkers::new(scope.registrar().downgrade());
    for _ in 0..4 {
        workers.spawn(async { Ok(()) }).unwrap();
        let exit = scope.next_receipt().await.unwrap();
        workers.reap();
        assert!(workers.state.lock().unwrap().records.is_empty());
        scope.acknowledge(&exit).unwrap();
    }
    workers.spawn(std::future::pending()).unwrap();
    let (_, witness) = workers.state.lock().unwrap().records[0]
        .registered()
        .unwrap();
    drop(scope);
    pending(witness.wait()).await;
    pending(workers.join()).await;
    assert!(!workers.is_drained());
}

#[tokio::test]
async fn observed_real_io_failure_preserves_every_decision_and_the_original_os_object() {
    use zero_core::{
        channel::{DeliveryState, RetryClass},
        component::{ComponentId, SessionId},
    };
    let mut scope = hosted(1);
    let workers = ScopedWorkers::new(scope.registrar().downgrade());
    let mut failure = ChannelError::new(Stage::Send, Reason::Io, Impact::Host)
        .with_source(std::io::Error::from_raw_os_error(10013));
    failure.component = Some(ComponentId(7));
    failure.session = Some(SessionId(11));
    failure.delivery = DeliveryState::LocallyAccepted;
    failure.retry = RetryClass::Revalidate;
    workers.spawn(async move { Err(failure) }).unwrap();
    let exit = scope.next_receipt().await.unwrap();
    let original = exit.result().unwrap_err();
    let forwarded = workers.join().await.unwrap_err();
    assert_eq!(
        (
            forwarded.stage,
            forwarded.reason,
            forwarded.impact,
            forwarded.component,
            forwarded.session,
            forwarded.delivery,
            forwarded.retry
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
    let shared = forwarded
        .source()
        .unwrap()
        .downcast_ref::<SharedFailure>()
        .unwrap();
    assert!(Arc::ptr_eq(&shared.0, &original));
    let actual = shared
        .0
        .source()
        .unwrap()
        .downcast_ref::<std::io::Error>()
        .unwrap();
    assert!(std::ptr::eq(
        actual,
        original
            .source()
            .unwrap()
            .downcast_ref::<std::io::Error>()
            .unwrap()
    ));
    assert_eq!(actual.raw_os_error(), Some(10013));
    scope.acknowledge(&exit).unwrap();
}

#[tokio::test]
async fn suspended_and_expired_host_registration_keep_the_actual_rejection_source() {
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    let workers = ScopedWorkers::new(scope.registrar().downgrade());
    let error = workers.spawn(async { Ok(()) }).unwrap_err();
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailure>()
        .unwrap();
    assert_eq!(
        shared.0.source().unwrap().downcast_ref::<SpawnError>(),
        Some(&SpawnError::NotStarted)
    );
    assert!(workers.join().await.is_err());
    assert!(workers.is_drained());
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    let weak = scope.registrar().downgrade();
    drop(scope);
    let expired = ScopedWorkers::new(weak);
    let error = expired.spawn(async { Ok(()) }).unwrap_err();
    let shared = error
        .source()
        .unwrap()
        .downcast_ref::<SharedFailure>()
        .unwrap();
    assert_eq!(
        shared.0.source().unwrap().downcast_ref::<SpawnError>(),
        Some(&SpawnError::Closing)
    );
    assert!(expired.join().await.is_err());
    assert!(expired.is_drained());
}

impl ScopedWorkers {
    pub(crate) fn test_original_witnesses(&self) -> Vec<TaskJoinWitness> {
        self.state
            .lock()
            .unwrap()
            .records
            .iter()
            .filter_map(|record| record.registered().map(|(_, witness)| witness))
            .collect()
    }
}
