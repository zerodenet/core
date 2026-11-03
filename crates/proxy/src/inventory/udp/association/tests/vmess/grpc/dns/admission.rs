use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_domain_without_native_dns_retains_legacy_udp_selection() {
    let dns = Dns::new().await;
    let endpoint = Endpoint::new().await;
    for config_dns in [
        None,
        Some(zero_config::DnsConfig {
            servers: std::collections::BTreeMap::from([(
                "system".into(),
                zero_config::DnsServerConfig::System,
            )]),
            default_server: "system".into(),
            ..dns.config()
        }),
    ] {
        let mut config = endpoint.config(&dns, false);
        config.runtime.dns = config_dns;
        let proxy = Proxy::new(config.clone()).unwrap();
        let (resolved, _) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("node".into()))
            .unwrap();
        let zero_engine::ResolvedOutbound::Single(leaf) = resolved else {
            panic!("single")
        };
        let claim = proxy.protocols.claim_outbound_leaf(&config, leaf).unwrap();
        assert!(claim.into_udp_flow_for_route(RouteMode::Flow).is_ok());
    }
    dns.close().await;
}

#[tokio::test]
async fn ordinary_vmess_grpc_domain_dns_failure_never_dials_tls_or_falls_back_to_legacy_udp() {
    timeout(Duration::from_secs(12), async {
        let mut dns = Dns::new().await;
        let endpoint = Endpoint::new().await;
        let (proxy, running, _) = start_domain(endpoint.config(&dns, false)).await;
        let opening = {
            let proxy = proxy.clone();
            tokio::spawn(async move {
                let mut dispatch = UdpIngressRuntime::new(proxy.tcp_runtime_services())
                    .new_dispatch("incoming")
                    .await
                    .unwrap();
                let result = dispatch.dispatch(input(53, b"owned")).await;
                assert!(result.is_err());
                dispatch.finish_all();
            })
        };
        let request = dns.request().await;
        let query = pending(&proxy).await;
        request.answer(false, true);
        opening.await.unwrap();
        query.task.wait().await;
        dns_closed(&query);
        endpoint.assert_no_connection().await;
        assert!(original(&proxy)
            .observe()
            .unwrap()
            .slots()
            .iter()
            .all(|slot| slot.native().is_none()));
        assert!(proxy.engine().active_sessions().is_empty());
        proxy
            .tcp_runtime_services()
            .execution()
            .retired_datagrams()
            .wait_closed(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}
