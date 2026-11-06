//! The paid declaration and actual actor share one book through Host ACK.
use super::*;

fn instance() -> ComponentInstanceId {
    ComponentInstanceId {
        component: ComponentId(902),
        generation: ResourceGeneration(1),
    }
}

#[tokio::test]
async fn sealed_paid_declaration_waits_for_its_actual_actor_join_and_deferred_host_ack() {
    let budget = ProcessBudget::new(ProcessBudgetLimits {
        max_jobs: 1,
        max_jobs_per_instance: 1,
    })
    .unwrap();
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let paid = budget.reserve_declared(instance(), 0).unwrap();
    paid.bind_scope(scope.registrar().scope_identity()).unwrap();
    let (stop, stopped) = oneshot::channel();
    let (_, actor) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            stopped.await.unwrap();
            Ok(())
        })
        .unwrap();
    paid.bind_actor(&actor).unwrap();
    paid.request_stop();
    drop(paid);
    assert_eq!(
        budget.snapshot().charged_jobs,
        1,
        "Drop and cancellation are no actor receipts"
    );
    assert!(budget.reserve_declared(instance(), 1).is_err());
    stop.send(()).unwrap();
    let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    assert!(receipt.result().is_ok());
    assert!(actor.matches_joined(receipt.joined()));
    assert_eq!(
        budget.snapshot().charged_jobs,
        1,
        "actual join still owes Host ACK"
    );
    let callbacks = scope.defer_callbacks();
    scope.acknowledge(&receipt).unwrap();
    assert!(!receipt.joined().registration_released());
    assert_eq!(budget.snapshot().charged_jobs, 1);
    drop(callbacks);
    assert!(receipt.joined().registration_released());
    assert_eq!(budget.snapshot().charged_jobs, 0);
    scope.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
}

#[tokio::test]
async fn paid_transfer_rejects_a_foreign_scope_and_keeps_same_cold_original_until_actual_actor_ack()
{
    let budget = ProcessBudget::new(ProcessBudgetLimits {
        max_jobs: 1,
        max_jobs_per_instance: 1,
    })
    .unwrap();
    let mut scope =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    scope.enable_registration().unwrap();
    let mut foreign =
        TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
    foreign.enable_registration().unwrap();
    let paid = budget.reserve_declared(instance(), 0).unwrap();
    paid.bind_scope(scope.registrar().scope_identity()).unwrap();
    let (stop, stopped) = oneshot::channel();
    let (_, actor) = scope
        .registrar()
        .spawn_tracked(TaskClass::Critical, async move {
            stopped.await.unwrap();
            Ok(())
        })
        .unwrap();
    paid.bind_actor(&actor).unwrap();
    let wrong = OwnedProcess::prepare(ProcessRequest::new("unused-no-spawn"), &foreign.registrar())
        .unwrap();
    assert_eq!(
        budget
            .reserve_prepaid(instance(), 0, &wrong, &foreign.registrar(), &paid)
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    wrong.request_stop();
    assert!(!wrong
        .try_finish()
        .unwrap()
        .unwrap()
        .had_native_acquisition());
    assert_eq!(budget.snapshot().charged_jobs, 1);
    let original =
        OwnedProcess::prepare(ProcessRequest::new("unused-no-spawn"), &scope.registrar()).unwrap();
    budget
        .reserve_prepaid(instance(), 0, &original, &scope.registrar(), &paid)
        .unwrap();
    assert!(budget.pending().is_empty());
    assert_eq!(budget.snapshot().charged_jobs, 1);
    paid.request_stop();
    original.request_stop();
    let closed = original.try_finish().unwrap().unwrap();
    assert!(closed.matches(&original) && !closed.had_native_acquisition());
    assert_eq!(
        budget.snapshot().charged_jobs,
        1,
        "same transferred cold original still owes actual actor ACK"
    );
    stop.send(()).unwrap();
    let receipt = tokio::time::timeout_at(end(), scope.next_receipt())
        .await
        .unwrap()
        .unwrap();
    receipt.result().unwrap();
    assert_eq!(budget.snapshot().charged_jobs, 1);
    scope.acknowledge(&receipt).unwrap();
    assert_eq!(budget.snapshot().charged_jobs, 0);
    scope.begin_shutdown();
    foreign.begin_shutdown();
    assert!(scope.next_receipt().await.is_none());
    assert!(foreign.next_receipt().await.is_none());
}
