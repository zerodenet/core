use super::*;
use std::{
    future::Future,
    task::{Context, Waker},
};
use tokio::sync::oneshot;
mod errors;
fn scope(limit: usize) -> TaskScope {
    TaskScope::new(Handle::current(), NonZeroUsize::new(limit).unwrap())
}
fn pending(witness: &TaskJoinWitness) {
    let mut wait = Box::pin(witness.wait());
    assert!(wait
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
}
struct BodyDropped(Option<oneshot::Sender<()>>);
impl Drop for BodyDropped {
    fn drop(&mut self) {
        if let Some(done) = self.0.take() {
            let _ = done.send(());
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_finished_task_body_is_not_joined_until_the_scope_consumes_its_exit() {
    let mut owner = scope(1);
    let (body_dropped, observed) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            let _body = BodyDropped(Some(body_dropped));
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    assert!(!witness.is_joined());
    assert!(witness.try_joined().is_none());
    pending(&witness);
    assert_eq!(
        owner.0.state.lock().unwrap().tasks.len(),
        1,
        "the native exit is still unconsumed"
    );
    let exit = owner.next_exit().await.unwrap();
    assert_eq!(exit.id, id);
    let receipt = witness.wait().await;
    assert_eq!(receipt.id(), id);
    assert_eq!(receipt.class(), TaskClass::Critical);
    receipt.result().unwrap();
    assert!(witness.is_joined());
}

#[tokio::test]
async fn cancelling_a_pending_wait_keeps_the_same_real_task_for_retry_and_multiple_waiters() {
    let mut owner = scope(1);
    let (release, block) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            block.await.unwrap();
            Ok(())
        })
        .unwrap();
    pending(&witness); // Construct, poll and drop a real pending wait.
    assert!(!witness.is_joined());
    release.send(()).unwrap();
    let other = witness.clone();
    let (left, right, exit) = tokio::join!(witness.wait(), other.wait(), owner.next_exit());
    assert_eq!(exit.unwrap().id, id);
    assert_eq!(left.id(), id);
    assert_eq!(right.class(), TaskClass::Auxiliary);
    assert!(Arc::ptr_eq(&left.0, &right.0));
    assert!(left.result().is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn observing_a_join_does_not_acknowledge_the_supervisors_event_or_release_its_budget() {
    let mut owner = scope(1);
    let register = owner.registrar();
    let (done, observed) = oneshot::channel();
    let (id, witness) = register
        .spawn_tracked(TaskClass::Critical, async move {
            let _body = BodyDropped(Some(done));
            Ok(())
        })
        .unwrap();
    observed.await.unwrap();
    assert!(!witness.is_joined());
    // Existing registration reaps actual exits, but undelivered supervisor
    // events still count. The receipt alone never consumes that event.
    assert_eq!(
        register.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    assert!(witness.is_joined());
    assert_eq!(witness.wait().await.id(), id);
    assert!(owner.0.state.lock().unwrap().records.is_empty());
    assert_eq!(owner.0.state.lock().unwrap().completed.len(), 1);
    drop(witness);
    assert_eq!(
        register.spawn(TaskClass::Auxiliary, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    assert_eq!(owner.next_exit().await.unwrap().id, id);
    let (next, receipt) = register
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    assert_ne!(id, next);
    assert_eq!(owner.next_exit().await.unwrap().id, next);
    assert_eq!(receipt.wait().await.id(), next);
}

#[tokio::test]
async fn dropping_the_scope_or_actual_task_future_never_issues_a_join_receipt() {
    let owner = scope(1);
    let register = owner.registrar();
    let (entered, started) = oneshot::channel();
    let (dropped, observed) = oneshot::channel();
    let (_, witness) = register
        .spawn_tracked(TaskClass::Critical, async move {
            let _body = BodyDropped(Some(dropped));
            entered.send(()).unwrap();
            std::future::pending::<Result<(), ComponentError>>().await
        })
        .unwrap();
    started.await.unwrap();
    pending(&witness);
    drop(owner); // Emergency abort only, with no actual JoinSet consumer.
    observed.await.unwrap();
    assert!(!witness.is_joined());
    pending(&witness);
    assert_eq!(
        register.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
    drop(register);
    assert!(!witness.is_joined());
    pending(&witness);
}

#[tokio::test]
async fn records_hold_only_weak_witnesses_and_consumed_exits_leave_no_scope_history() {
    let mut owner = scope(1);
    let (release, wait) = oneshot::channel();
    let (_, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async move {
            wait.await.unwrap();
            Ok(())
        })
        .unwrap();
    let weak = Arc::downgrade(&witness.0);
    assert_eq!(Arc::strong_count(&witness.0), 1);
    drop(witness);
    assert!(weak.upgrade().is_none());
    assert!(owner
        .0
        .state
        .lock()
        .unwrap()
        .records
        .values()
        .next()
        .unwrap()
        .witness
        .as_ref()
        .unwrap()
        .upgrade()
        .is_none());
    release.send(()).unwrap();
    owner.next_exit().await.unwrap();
    let state = owner.0.state.lock().unwrap();
    assert!(state.records.is_empty());
    assert!(state.completed.is_empty());
    assert!(state.tasks.is_empty());
    assert!(state.closing_report.is_none());
}

#[tokio::test]
async fn interrupted_shutdown_and_deadline_keep_the_actual_task_and_witness_for_retry() {
    let mut owner = scope(1);
    let (entered, started) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let (id, witness) = owner
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            entered.send(()).unwrap();
            wait.await.unwrap();
            Ok(())
        })
        .unwrap();
    started.await.unwrap();
    let mut shutdown =
        Box::pin(owner.shutdown(Instant::now() + std::time::Duration::from_secs(60)));
    assert!(shutdown
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    drop(shutdown); // Interrupt the actual pending supervisor, retaining its owner.
    assert!(!witness.is_joined());
    let report = owner.shutdown(Instant::now()).await;
    assert!(!report.is_complete());
    assert!(matches!(report.unfinished.as_slice(), [Unfinished::Task(task)] if *task == id));
    pending(&witness);
    assert_eq!(owner.0.state.lock().unwrap().records.len(), 1);
    release.send(()).unwrap();
    let report = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks, [id]);
    assert_eq!(witness.wait().await.id(), id);
}

#[tokio::test]
async fn tracked_registration_preserves_cold_capacity_and_closing_reasons_without_polling_rejected_tasks(
) {
    let mut owner = TaskScope::suspended(Handle::current(), Some(NonZeroUsize::new(1).unwrap()));
    let register = owner.registrar();
    assert_eq!(
        register
            .spawn_tracked(TaskClass::Critical, async {
                panic!("must not poll cold task")
            })
            .unwrap_err(),
        SpawnError::NotStarted
    );
    owner.enable_registration().unwrap();
    let cancel = register.cancellation();
    let (id, witness) = register
        .spawn_tracked(TaskClass::Critical, async move {
            cancel.cancelled().await;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        register
            .spawn_tracked(TaskClass::Critical, async {
                panic!("must not poll overflow task")
            })
            .unwrap_err(),
        SpawnError::Capacity
    );
    let report = owner
        .shutdown(Instant::now() + std::time::Duration::from_secs(1))
        .await;
    assert!(report.is_complete());
    assert_eq!(report.exited_tasks, [id]);
    assert_eq!(witness.wait().await.id(), id);
    assert_eq!(
        register
            .spawn_tracked(TaskClass::Critical, async {
                panic!("must not poll closing task")
            })
            .unwrap_err(),
        SpawnError::Closing
    );
}

#[tokio::test]
async fn witness_identity_is_the_original_registration_before_and_after_real_join() {
    let mut first = scope(2);
    let mut second = scope(1);
    let (_, a) = first
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let (_, b) = first
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let (_, foreign) = second
        .registrar()
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let same = a.clone();
    assert!(a.same_registration(&same));
    assert!(!a.same_registration(&b));
    assert!(!a.same_registration(&foreign));
    assert!(!a.is_joined());
    first.next_exit().await.unwrap();
    first.next_exit().await.unwrap();
    second.next_exit().await.unwrap();
    assert!(a.same_registration(&same));
    assert!(!a.same_registration(&b));
    assert!(!a.same_registration(&foreign));
    assert_eq!(
        a.try_joined().unwrap().id(),
        same.try_joined().unwrap().id()
    );
}
