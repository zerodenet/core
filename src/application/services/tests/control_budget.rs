//! The production application consumes the Proxy's actual control candidate.
use super::*;
#[tokio::test]
async fn same_actual_proxy_application_book_changes_once_at_configuration_publication() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let config = RuntimeConfig::parse(
            r#"{"runtime":{"control_max_pending_commands":1},"route":{"final":{"type":"direct"}}}"#,
        )
        .unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let services = ApplicationServices::start_with_proxy(&proxy, None)
            .await
            .unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &services.control_budget(),
            &proxy.application_control_budget()
        ));
        let handle = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone())
            .with_config_apply_reconciler(services.clone());
        handle
            .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(3))
            .await
            .unwrap();
        let mut target = config;
        target.runtime.control_max_pending_commands = Some(2);
        handle
            .apply_runtime_config_and_wait(target, Duration::from_secs(3))
            .await
            .unwrap();
        assert_eq!(
            services.control_budget().limits().messages.pending_commands,
            2
        );
        assert!(services.control_budget().retained_candidate().is_none());
        services.shutdown_control().await.unwrap();
        services.shutdown_status_monitor().await.unwrap();
        services.shutdown_dispatcher().await.unwrap();
        running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
