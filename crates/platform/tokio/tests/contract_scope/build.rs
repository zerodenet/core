use super::*;

#[tokio::test]
async fn suspended_component_registrar_requires_the_actual_owner_to_enter_start() {
    let mut owner = TaskScope::suspended(Handle::current(), NonZeroUsize::new(1));
    let registrar = owner.registrar();
    let polled = Arc::new(AtomicBool::new(false));
    let rejected = polled.clone();
    assert_eq!(
        registrar.spawn(TaskClass::Critical, async move {
            rejected.store(true, Ordering::SeqCst);
            Ok(())
        }),
        Err(SpawnError::NotStarted)
    );
    assert!(!polled.load(Ordering::SeqCst));
    owner.enable_registration().unwrap();
    let task = registrar
        .spawn(TaskClass::Critical, async move {
            polled.store(true, Ordering::SeqCst);
            Ok(())
        })
        .unwrap();
    let exit = owner.next_exit().await.unwrap();
    assert_eq!(exit.id, task);
    assert!(exit.result.is_ok());
    owner.begin_shutdown();
    assert_eq!(owner.enable_registration(), Err(SpawnError::Closing));
    assert_eq!(
        registrar.spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Closing)
    );
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .is_complete());
}

#[tokio::test]
async fn enabling_a_zero_task_component_does_not_invent_a_task_budget() {
    let mut owner = TaskScope::suspended(Handle::current(), None);
    assert_eq!(
        owner
            .registrar()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::NotStarted)
    );
    owner.enable_registration().unwrap();
    assert_eq!(
        owner
            .registrar()
            .spawn(TaskClass::Critical, async { Ok(()) }),
        Err(SpawnError::Capacity)
    );
    assert!(owner
        .shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .is_complete());
}
