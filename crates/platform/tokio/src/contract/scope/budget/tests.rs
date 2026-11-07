use super::*;
use tokio::sync::oneshot;
fn scope(book: &Arc<TaskRegistrationBudget>) -> HostedScope {
    let mut scope = TaskScope::suspended_hosted(Handle::current(), NonZeroUsize::new(2));
    scope.bind_task_budget(book.clone()).unwrap();
    scope
}
#[tokio::test]
async fn two_actual_scopes_share_credit_until_the_same_native_exit_and_host_ack() {
    timeout_at(Instant::now() + std::time::Duration::from_secs(3), async {
        let book = TaskRegistrationBudget::new(NonZeroUsize::new(1).unwrap());
        let mut first = scope(&book);
        let mut second = scope(&book);
        first.enable_registration().unwrap();
        second.enable_registration().unwrap();
        let (release, gate) = oneshot::channel();
        let (_, original) = first
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                gate.await.unwrap();
                Ok(())
            })
            .unwrap();
        assert!(original.native_task_id().is_some());
        assert!(!original.is_joined());
        assert_eq!(book.usage().charged, 1);
        assert!(matches!(
            second
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        ));
        release.send(()).unwrap();
        let receipt = first.next_receipt().await.unwrap();
        let joined = original.wait().await;
        assert!(original.matches_joined(&joined));
        assert!(!joined.registration_released());
        assert_eq!(
            book.usage().charged,
            1,
            "body/native join alone cannot return registration credit"
        );
        assert!(second.acknowledge(&receipt).is_err());
        assert_eq!(book.usage().charged, 1);
        assert!(matches!(
            second
                .registrar()
                .spawn(TaskClass::Auxiliary, async { Ok(()) }),
            Err(SpawnError::Capacity)
        ));
        first.acknowledge(&receipt).unwrap();
        original.wait_registration_released().await;
        assert_eq!(book.usage().charged, 0);
        let (_, later) = second
            .registrar()
            .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
            .unwrap();
        assert_ne!(original.native_task_id(), later.native_task_id());
        let receipt = second.next_receipt().await.unwrap();
        second.acknowledge(&receipt).unwrap();
        later.wait_registration_released().await;
        assert_eq!(book.usage().charged, 0);
        first.begin_shutdown();
        second.begin_shutdown();
        assert!(first.is_drained() && second.is_drained());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn cold_launch_prepays_both_limits_but_cannot_spawn_before_the_original_host_start() {
    timeout_at(Instant::now() + std::time::Duration::from_secs(3), async {
        let book = TaskRegistrationBudget::new(NonZeroUsize::new(1).unwrap());
        let mut first = scope(&book);
        let mut other = scope(&book);
        let launch = first
            .registrar()
            .reserve_launch(TaskClass::Critical)
            .unwrap();
        assert_eq!(book.usage().charged, 1);
        assert!(!first.is_joined());
        assert!(matches!(
            other.registrar().reserve_launch(TaskClass::Critical),
            Err(SpawnError::Capacity)
        ));
        assert!(matches!(
            launch.spawn_tracked(async { panic!("unstarted actual scope cannot run work") }),
            Err(SpawnError::NotStarted)
        ));
        assert_eq!(
            book.usage().charged,
            0,
            "positively never spawned launch may roll back"
        );
        let launch = first
            .registrar()
            .reserve_launch(TaskClass::Critical)
            .unwrap();
        first.enable_registration().unwrap();
        let (_, original) = launch.spawn_blocking_tracked(|| Ok(())).unwrap();
        let receipt = first.next_receipt().await.unwrap();
        assert!(original.native_task_id().is_some());
        assert_eq!(book.usage().charged, 1);
        first.acknowledge(&receipt).unwrap();
        original.wait_registration_released().await;
        assert_eq!(book.usage().charged, 0);
        let unstarted = other
            .registrar()
            .reserve_launch(TaskClass::Critical)
            .unwrap();
        other.begin_shutdown();
        assert!(!other.is_drained());
        drop(unstarted);
        assert!(other.is_drained());
        first.begin_shutdown();
        assert!(first.is_drained());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn dropping_an_original_scope_and_aborting_its_body_does_not_claim_global_ack() {
    let book = TaskRegistrationBudget::new(NonZeroUsize::new(1).unwrap());
    let mut original = scope(&book);
    original.enable_registration().unwrap();
    let (_, witness) = original
        .registrar()
        .spawn_tracked(TaskClass::Critical, std::future::pending())
        .unwrap();
    assert!(witness.native_task_id().is_some());
    assert_eq!(book.usage().charged, 1);
    drop(original);
    tokio::task::yield_now().await;
    assert!(!witness.is_joined());
    assert_eq!(book.usage().charged, 1);
    let mut replacement = scope(&book);
    replacement.enable_registration().unwrap();
    assert!(matches!(
        replacement
            .registrar()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    ));
    replacement.begin_shutdown();
    assert!(replacement.is_drained());
    // Deliberate lost-owner debt, not a recovery claim or native close proof.
}
