//! Native qualification uses the real privileged backend, never a file as TUN.
use super::*;
use crate::runtime::inbound_owners::system_tun::SYSTEM_TUN_OWNER;
use crate::ProxyHandle;
use std::time::Duration;

#[cfg(any(target_os = "linux", target_os = "macos", target_os = "windows"))]
#[tokio::test]
#[ignore = "requires actual privileged TUN backend and configured native SDK; no substitute native fixture"]
async fn ordinary_command_system_tun_budget_replacement_keeps_intent_after_matching_native_and_scope_close(
) {
    let config =
        zero_config::RuntimeConfig::parse(r#"{"route":{"final":{"type":"direct"}}}"#).unwrap();
    let proxy = Proxy::new(config.clone()).unwrap();
    let handle = ProxyHandle::new(
        zero_engine::EngineHandle::new(proxy.engine().clone()),
        proxy.clone(),
    );
    let mut ready = proxy.orchestration_readiness();
    let running = proxy.spawn();
    tokio::time::timeout(Duration::from_secs(5), ready.wait_for(|ready| *ready))
        .await
        .unwrap()
        .unwrap();
    let name = format!("zrb{}", std::process::id());
    proxy
        .start_tun(
            TunInterfaceOptions {
                name: Some(&name),
                addr: "10.203.77.1/30",
                mask: "",
                secondary_addr: Some("fd66:7a::1/126"),
            },
            1500,
            "budget-command-tun",
            TunRuntimeOptions {
                auto_route: false,
                include_cidrs: vec![],
                exclude_cidrs: vec![],
                dual_stack: true,
                strict_route: false,
                dns_hijack: true,
            },
        )
        .await
        .unwrap();
    let mut old = proxy
        .system_tun_source_registry()
        .owner(SYSTEM_TUN_OWNER)
        .unwrap();
    let requested = proxy.tun_info.lock().unwrap().clone().unwrap();
    assert!(requested.managed_config.is_none());
    for frames in [128, 256] {
        let main = old.task_access().source_main_witness().await.unwrap();
        let completion = proxy
            .tun_control
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .completion
            .clone();
        let original = completion
            .source_original()
            .expect("same actual native provider");
        assert!(!main.is_joined());
        assert!(!original.driver().is_joined());
        assert!(original.closed().is_none());
        let mut changed = config.clone();
        changed.runtime.packet_return_max_frames = Some(frames);
        handle
            .apply_runtime_config_and_wait(changed, Duration::from_secs(5))
            .await
            .unwrap();
        let current = proxy
            .system_tun_source_registry()
            .owner(SYSTEM_TUN_OWNER)
            .unwrap();
        current.wait_ready().await.unwrap();
        assert!(!Arc::ptr_eq(&old, &current));
        assert!(main.try_joined().unwrap().registration_released());
        assert!(original
            .driver()
            .try_joined()
            .unwrap()
            .registration_released());
        assert!(original.closed().unwrap().matches(&original));
        assert!(completion.resources_complete().await);
        let restored = proxy.tun_info.lock().unwrap().clone().unwrap();
        assert!(restored.managed_config.is_none());
        assert_ne!(requested.id, restored.id);
        assert_eq!(restored.name, requested.name);
        assert_eq!(restored.addr, requested.addr);
        assert_eq!(restored.addresses, requested.addresses);
        assert_eq!(
            (
                restored.mtu,
                &restored.tag,
                restored.dual_stack,
                restored.dns_hijack
            ),
            (
                requested.mtu,
                &requested.tag,
                requested.dual_stack,
                requested.dns_hijack
            )
        );
        assert!(Arc::ptr_eq(
            &restored.dns_hijacked_queries,
            &requested.dns_hijacked_queries
        ));
        assert!(
            !proxy.has_tun_packet_budget_recovery(),
            "only actual matching commit clears the capsule"
        );
        old = current;
    }
    proxy.stop_tun().await.unwrap();
    assert!(proxy.tun_control.lock().unwrap().is_none());
    tokio::time::timeout(Duration::from_secs(5), running.shutdown())
        .await
        .unwrap()
        .unwrap();
}
