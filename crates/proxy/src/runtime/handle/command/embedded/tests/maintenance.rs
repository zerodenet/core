//! Async maintenance must never reenter block_on inside the command owner.
use super::*;
use zero_api::{
    CommandRequest, DiagnosticsDnsCacheCommand, DiagnosticsFakeipLookupCommand, FakeIpClearCommand,
    TunRecoverCommand, TunStartCommand, TunStopCommand,
};
use zero_traits::DnsResolver;

#[tokio::test]
async fn acknowledged_maintenance_keeps_async_ownership_on_a_current_thread_runtime() {
    timeout(Duration::from_secs(10), async {
        let config = zero_config::RuntimeConfig::parse(
            r#"{
            "runtime":{"dns":{"servers":{"system":{"type":"system"}},
                "default_server":"system","answer":{"type":"fake_ip",
                "cidr":"198.18.0.0/24","ttl_seconds":60,"max_entries":16}}},
            "route":{"final":{"type":"direct"}}
        }"#,
        )
        .unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        DnsResolver::resolve(proxy.resolver.as_ref(), "maintenance.example")
            .await
            .unwrap();
        assert!(proxy
            .resolver
            .lookup_fake_ip_domain("maintenance.example")
            .await
            .is_some());
        for command in [
            CommandRequest::DiagnosticsDnsCache(DiagnosticsDnsCacheCommand::default()),
            CommandRequest::DiagnosticsFakeipLookup(DiagnosticsFakeipLookupCommand {
                domain: Some("maintenance.example".into()),
                ip: None,
            }),
            CommandRequest::FakeIpClear(FakeIpClearCommand::default()),
        ] {
            let observation = zero_runtime::ApplyObservation::new();
            let result = observation
                .scope(handle.execute_acknowledged(command))
                .await
                .unwrap();
            assert!(result.accepted);
            assert!(observation.blocking_outcome().is_none());
        }
        assert!(proxy
            .resolver
            .lookup_fake_ip_domain("maintenance.example")
            .await
            .is_none());
        let observation = zero_runtime::ApplyObservation::new();
        let stopped = observation
            .scope(handle.execute_acknowledged(CommandRequest::TunStop(TunStopCommand {})))
            .await
            .expect_err("no native TUN has been acquired");
        assert!(stopped.message.contains("TUN is not running"));
        assert!(observation.blocking_outcome().is_none());
        let result = handle
            .execute_acknowledged(CommandRequest::TunRecover(TunRecoverCommand {}))
            .await
            .expect_err("no native TUN has been acquired");
        assert!(result
            .message
            .contains("TUN automatic routes are not running"));
        let invalid_start: TunStartCommand = serde_json::from_value(serde_json::json!({
            "addr":"198.18.0.1", "mask":"255.255.255.0", "tag":"tun",
            "auto_route":true, "include_cidrs":["invalid-prefix"]
        }))
        .unwrap();
        let result = handle
            .execute_acknowledged(CommandRequest::TunStart(invalid_start))
            .await
            .unwrap_err();
        assert_eq!(result.code, zero_api::ApiErrorCode::InvalidArgument);
        proxy
            .protocols
            .drive_component_supervision(
                proxy
                    .embedded_command_owner()
                    .shutdown(tokio::time::Instant::now() + Duration::from_secs(5)),
            )
            .await
            .unwrap();
        assert!(proxy.embedded_command_owner().retirement_complete());
        assert_eq!(
            proxy.application_control_budget().usage().pending_commands,
            0
        );
        running.shutdown().await.unwrap();
        assert_eq!(proxy.host.original_budgets().carrier.snapshot().charged, 0);
    })
    .await
    .unwrap();
}

#[cfg(all(feature = "raw-ip-runtime", feature = "mixed"))]
#[tokio::test]
async fn outbound_reconciliation_barrier_waits_until_services_restore_generation() {
    timeout(Duration::from_secs(10), async {
        let config =
            zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
        let proxy = Proxy::new(config).unwrap();
        let running = proxy.spawn();
        running
            .wait_ready(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
            .unwrap();

        proxy.egress_interface.invalidate_network();
        let target = proxy.egress_interface.generation();
        assert!(target > 0);
        crate::runtime::handle::command::tun::wait_for_outbound_reconciliation(&proxy)
            .await
            .unwrap();
        assert!(*proxy.reconciled_device_generations().borrow() >= target);

        running.shutdown().await.unwrap();
        proxy.egress_interface.invalidate_network();
        crate::runtime::handle::command::tun::wait_for_outbound_reconciliation(&proxy)
            .await
            .unwrap();
    })
    .await
    .unwrap();
}
