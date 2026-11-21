//! TCP-only plugin config never starts a child for the existing plain UDP wire.
use super::*;

#[tokio::test]
async fn tcp_plugin_outer_uses_actual_udp_owner_without_starting_or_fabricating_a_process() {
    let remote_config = RuntimeConfig::parse(
        r#"{
        "inbounds":[{"tag":"ss-in","listen":{"address":"127.0.0.1","port":0},
            "protocol":{"type":"shadowsocks","cipher":"aes-128-gcm","password":"same-secret"}}],
        "route":{"final":{"type":"direct"}}
    }"#,
    )
    .unwrap();
    let remote = crate::Proxy::new(remote_config.clone()).unwrap();
    let remote_running = remote.spawn();
    handle(&remote)
        .apply_runtime_config_and_wait(remote_config, Duration::from_secs(5))
        .await
        .unwrap();
    let source = remote.system_tun_source_registry().owner("ss-in").unwrap();
    source.wait_ready().await.unwrap();
    let endpoint = *source.original_listener_address.get().unwrap();
    let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    for linked in [false, true] {
        let mut candidate =
            serde_json::to_value(config(peer.local_addr().unwrap(), None, linked)).unwrap();
        candidate["outbounds"][0]["protocol"]["outer_udp_proxy"] = serde_json::json!("ss");
        candidate["outbounds"].as_array_mut().unwrap().push(serde_json::json!({
            "tag":"ss","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":endpoint.port(),
                "cipher":"aes-128-gcm","password":"same-secret",
                "plugin":{"command":"/nonexistent/must-not-spawn-for-udp","mode":"tcp_only"}}
        }));
        let candidate = RuntimeConfig::parse(&candidate.to_string()).unwrap();
        let proxy = crate::Proxy::new(candidate.clone()).unwrap();
        let running = proxy.spawn();
        handle(&proxy)
            .apply_runtime_config_and_wait(candidate.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        let plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let slot = plan.configured("ss").unwrap();
        assert_eq!(
            slot.recipe.spec().roles,
            vec![zero_platform_tokio::contract::component::RoleKind::Datagram]
        );
        assert!(slot
            .recipe
            .spec()
            .resources
            .iter()
            .all(|resource| !matches!(
                resource,
                zero_platform_tokio::contract::component::ResourceDeclaration::Process { .. }
            )));
        let mut packet = [0; 2048];
        let (size, _) = tokio::time::timeout(Duration::from_secs(5), peer.recv_from(&mut packet))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(size, 148);
        assert_eq!(&packet[..4], &[1, 0, 0, 0]);
        let old = devices(&proxy).remove(0);
        let old_workers = worker_census(&old);
        handle(&proxy)
            .apply_runtime_config_and_wait(changed(candidate), Duration::from_secs(5))
            .await
            .unwrap();
        joined(&old, &old_workers);
        running.shutdown().await.unwrap();
        while peer.try_recv_from(&mut packet).is_ok() {}
    }
    remote_running.shutdown().await.unwrap();
    let terminal = source.terminal_for_test().unwrap();
    assert!(terminal.failure().is_none() && terminal.closed_io_receipts() > 0);
}
