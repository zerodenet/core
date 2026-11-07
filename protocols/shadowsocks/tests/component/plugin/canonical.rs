//! Same actual canonical Process key; root reap is never tree/actor credit.
use super::*;
fn options(state: &crate::transport::ShadowsocksTransportState) -> PluginOptions {
    state
        .plugin_component_options(
            "127.0.0.1",
            9999,
            crate::transport::plugin::fixtures::config(),
        )
        .with_process_endpoint(zero_core::component::NetworkGeneration(0))
        .unwrap()
}
fn scoped(
    tasks: zero_platform_tokio::contract::scope::TaskRegistrar,
    grant: Arc<Grant>,
    options: PluginOptions,
    generation: u64,
) -> Result<ComponentInstance, BuildError> {
    PluginFactory.build(
        Arc::new(options),
        BuildContext {
            component: ComponentId(770),
            generation: ResourceGeneration(generation),
            dependencies: Arc::from([]),
            roles: Default::default(),
            resources: grant,
            tasks,
            cancel: CancellationToken::new(),
            background_carrier: None,
        },
    )
}
#[tokio::test]
async fn canonical_process_native_generation_needs_matching_native_and_real_deferred_actor_ack_before_new_io(
) {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let state = crate::transport::ShadowsocksTransportState::default();
        let mut old_scope =
            TaskScope::suspended_hosted(tokio::runtime::Handle::current(), NonZeroUsize::new(1));
        old_scope.enable_registration().unwrap();
        let old_grant = Arc::new(Grant::for_scope(&old_scope, 1));
        let mut old = scoped(old_scope.registrar(), old_grant.clone(), options(&state), 1).unwrap();
        old.lifecycle.start(deadline()).await.unwrap();
        let original = old_grant.originals.lock().unwrap()[0].clone();
        let actor = old_grant.actor.lock().unwrap().clone().unwrap();
        old.lifecycle.revoke(CloseReason::Shutdown);
        let receipt = old_scope.next_receipt().await.unwrap();
        assert!(receipt.result().is_ok());
        assert!(actor
            .try_joined()
            .is_some_and(|joined| !joined.registration_released()));
        old_scope.begin_shutdown();
        let closed = original.wait_closed(deadline()).await.unwrap().unwrap();
        assert!(closed.matches(&original));
        assert!(closed.had_native_acquisition());
        old_grant.confirm_closed(&old_scope);
        state.clear_plugins();
        let mut next_scope = TaskScope::new(
            tokio::runtime::Handle::current(),
            NonZeroUsize::new(1).unwrap(),
        );
        let next_grant = Arc::new(Grant::for_scope(&next_scope, 2));
        assert_eq!(
            scoped(
                next_scope.registrar(),
                next_grant.clone(),
                options(&state),
                2
            )
            .err()
            .unwrap()
            .reason,
            Reason::Capacity
        );
        assert_eq!(next_grant.starts.load(Ordering::SeqCst), 0);
        let deferred = old_scope.defer_callbacks();
        old_scope.acknowledge(&receipt).unwrap();
        assert!(!actor.try_joined().unwrap().registration_released());
        state.clear_plugins();
        assert_eq!(
            scoped(
                next_scope.registrar(),
                next_grant.clone(),
                options(&state),
                2
            )
            .err()
            .unwrap()
            .reason,
            Reason::Capacity
        );
        assert_eq!(next_grant.starts.load(Ordering::SeqCst), 0);
        drop(deferred);
        let released = actor.wait_registration_released().await;
        assert!(actor.matches_joined(&released) && released.registration_released());
        assert!(old
            .lifecycle
            .shutdown(deadline())
            .await
            .unwrap()
            .is_complete());
        state.clear_plugins();
        let mut next = scoped(
            next_scope.registrar(),
            next_grant.clone(),
            options(&state),
            2,
        )
        .unwrap();
        next.lifecycle.start(deadline()).await.unwrap();
        assert_eq!(next_grant.starts.load(Ordering::SeqCst), 1);
        let successor = next_grant.originals.lock().unwrap()[0].clone();
        assert!(!Arc::ptr_eq(&successor, &original));
        assert!(successor.closed().is_none());
        let (native, tasks) = tokio::join!(
            next.lifecycle.shutdown(deadline()),
            next_scope.shutdown(deadline())
        );
        assert!(native.unwrap().is_complete());
        assert!(tasks.is_complete());
        assert!(successor.closed().unwrap().matches(&successor));
        assert!(old_scope.is_drained());
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn real_standalone_library_root_reap_and_reload_never_invent_a_native_tree_handoff() {
    tokio::time::timeout(std::time::Duration::from_secs(8), async {
        let state = crate::transport::ShadowsocksTransportState::default();
        let resource = state.plugin_resource(
            "127.0.0.1",
            9999,
            crate::transport::plugin::fixtures::config(),
        );
        resource.start().await.unwrap();
        assert!(matches!(
            resource.shutdown(deadline()).await.unwrap(),
            PluginTermination::Exited(_)
        ));
        state.clear_plugins();
        let native = state
            .plugin_component_options(
                "127.0.0.1",
                9999,
                crate::transport::plugin::fixtures::config(),
            )
            .with_process_endpoint(zero_core::component::NetworkGeneration(0));
        assert!(
            native.is_err(),
            "only this genuine library root was reaped; no supervised-tree receipt exists"
        );
        // This conservative boundary is an explicit library-to-Host migration
        // gap. It is not a successful replacement or ordinary SIP qualification.
    })
    .await
    .unwrap();
}
