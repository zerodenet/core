use super::*;
#[tokio::test]
async fn actual_http_shutdown_retires_native_and_keeps_complete_scope_failure() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let budget = zero_runtime::configuration::ControlBudget::new(
            zero_runtime::configuration::ControlBudgetLimits::from_runtime(
                &zero_config::RuntimeOptionsConfig::default(),
            ),
        );
        let owner = prepare_http_server_with_budget("127.0.0.1:0", None, budget.clone());
        let mut scope = owner.scope.lock().await;
        let listener = bind_control_listener(
            "127.0.0.1:0",
            &mut scope,
            NonZeroUsize::new(1).unwrap(),
            NonZeroUsize::new(1).unwrap(),
            &owner.original,
        )
        .await
        .unwrap();
        drop(listener);
        let directory = tempfile::tempdir().unwrap();
        let actual = Arc::new(
            ChannelError::new(Stage::Close, Reason::Io, Impact::Component)
                .with_source(std::fs::File::open(directory.path().join("missing")).unwrap_err()),
        );
        let weak = Arc::downgrade(&actual);
        let source = actual.clone();
        let (_, witness) = scope
            .registrar()
            .spawn_tracked(TaskClass::Critical, async move {
                Err(ChannelError::retained(source))
            })
            .unwrap();
        drop(scope);
        let returned = owner.shutdown().await.unwrap_err();
        assert!(
            owner.retirement_complete(),
            "complete native/Scope reports retire despite the actual received fault"
        );
        assert!(witness.try_joined().unwrap().registration_released());
        assert_eq!(budget.usage().connections, 0);
        drop(actual);
        drop(witness);
        drop(owner);
        assert!(
            weak.upgrade().is_some(),
            "shutdown receiver retains exact source after server owner Drop"
        );
        drop(returned);
        assert!(weak.upgrade().is_none());
    })
    .await
    .unwrap();
}
