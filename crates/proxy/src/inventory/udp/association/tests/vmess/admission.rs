use super::*;
#[tokio::test]
async fn ordinary_vmess_datagram_keeps_udp_policy_packet_admission_and_revoked_role_fences() {
    for (enabled, mode, expected) in [
        (false, RouteMode::Flow, "udp_policy"),
        (true, RouteMode::Packet, "data_plane_route"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = proxy_with_udp(
            listener.local_addr().unwrap().port(),
            serde_json::json!({"final":{"type":"route","outbound":"node"}}),
            "aes-128-gcm",
            enabled,
        )
        .await;
        let config = proxy.engine().runtime_snapshot().config().clone();
        let (resolved, _plan) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("node".into()))
            .unwrap();
        let session = Session::new(
            7,
            Address::Ipv4([203, 0, 113, 9]),
            53,
            Network::Udp,
            ProtocolType::new("test"),
        );
        let failure = proxy
            .protocols
            .prepare_udp_outbound(
                UdpAdapterContext::new(&config, proxy.udp_runtime_services()),
                &session,
                &resolved,
                b"query",
                mode,
            )
            .err()
            .expect("policy rejection");
        assert_eq!(failure.stage, expected);
        assert!(timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err());
        proxy
            .protocols
            .shutdown_components(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy = proxy(
        listener.local_addr().unwrap().port(),
        serde_json::json!({"final":{"type":"route","outbound":"node"}}),
        "aes-128-gcm",
    )
    .await;
    let execution = proxy.tcp_runtime_services().execution();
    let slot = execution
        .components
        .as_ref()
        .unwrap()
        .configured("node")
        .unwrap();
    slot.revoke();
    let runtime = UdpIngressRuntime::new(proxy.tcp_runtime_services());
    let mut dispatch = runtime.new_dispatch("incoming").await.unwrap();
    let failure = dispatch.dispatch(input(53, b"query")).await.unwrap_err();
    assert_eq!(
        crate::transport::channel_failure(&failure).unwrap().reason,
        zero_platform_tokio::contract::Reason::StalePlan
    );
    assert!(
        timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err(),
        "no legacy redial after standard admission rejects"
    );
    let _ = dispatch.finish_all();
    proxy
        .protocols
        .shutdown_components(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}
