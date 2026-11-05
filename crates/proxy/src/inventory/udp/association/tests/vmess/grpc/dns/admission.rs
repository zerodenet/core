use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_domain_without_native_dns_retains_legacy_udp_selection() {
    let dns = Dns::new().await;
    let endpoint = Endpoint::new().await;
    let mut config = endpoint.config(&dns, false);
    config.runtime.dns = None;
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

#[tokio::test]
async fn ordinary_vmess_grpc_domain_system_node_dns_uses_the_actual_blocking_owner_and_trusted_udp()
{
    timeout(Duration::from_secs(15), async {
        let dns = Dns::new().await;
        let endpoint = Endpoint::new().await;
        let mut config = endpoint.config(&dns, false);
        config.runtime.dns = Some(zero_config::DnsConfig {
            servers: std::collections::BTreeMap::from([(
                "system".into(),
                zero_config::DnsServerConfig::System,
            )]),
            default_server: "system".into(),
            ..dns.config()
        });
        let zero_config::OutboundProtocolConfig::Vmess { server, tls, .. } =
            &mut config.outbounds[0].protocol
        else {
            unreachable!()
        };
        *server = "localhost".into();
        tls.as_mut().unwrap().server_name = Some(NAME.into());
        let (proxy, running, _) = start_domain(config).await;
        let peer = endpoint.serve(false, 1, vec![b"owned".to_vec()]);
        let dispatch = open(proxy.clone(), 1, b"owned").await;
        let registration = original(&proxy);
        let observed = registration.observe().unwrap();
        assert_eq!(observed.retained(), 1);
        let original = observed.slots()[0].original().clone();
        let driver = observed.slots()[0].driver().unwrap().1.clone();
        let completed = dispatch.finish_all();
        assert_eq!(completed[0].upstream.as_ref().unwrap().0, "localhost");
        assert!(proxy
            .tcp_runtime_services()
            .execution()
            .retired_datagrams()
            .progress_retired(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap()
            .is_complete());
        assert!(driver.matches_joined(&driver.try_joined().unwrap()));
        assert!(original.matches_closed(&original.closed().unwrap()));
        peer.await.unwrap();
        running.shutdown().await.unwrap();
        assert!(registration.users_closed().unwrap().matches(&registration));
        assert!(proxy
            .resolver
            .native_query_observations()
            .iter()
            .all(|query| query.task.is_joined()));
        dns.close().await;
    })
    .await
    .unwrap();
}
