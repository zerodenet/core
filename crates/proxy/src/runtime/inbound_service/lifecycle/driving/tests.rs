use super::*;
#[tokio::test]
async fn same_service_scope_is_driven_while_the_actual_configuration_operation_is_pending() {
    let owner = InboundServices::default();
    let budget = TaskRegistrationBudget::new(NonZeroUsize::new(2).unwrap());
    let registrar = {
        let mut state = owner.owner.lock().await;
        state
            .install(NonZeroUsize::new(2).unwrap(), budget.clone())
            .unwrap();
        state.scope.as_ref().unwrap().registrar()
    };
    let (_, witness) = registrar
        .spawn_tracked(TaskClass::Auxiliary, async { Ok(()) })
        .unwrap();
    let operation = async { witness.wait().await };
    let joined = tokio::time::timeout(std::time::Duration::from_secs(2), owner.drive(operation))
        .await
        .unwrap();
    assert!(joined.result().is_ok());
    assert_eq!(
        budget.usage().charged,
        0,
        "same Host ACK, not a separate observation task"
    );
    let mut clone = owner.clone();
    drop(owner);
    assert!(clone.owner.lock().await.scope.is_some());
    clone.shutdown().await.unwrap();
    assert!(clone.owner.lock().await.scope.is_none());
}
