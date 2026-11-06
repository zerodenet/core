//! Domain ECH keeps its actual Node bootstrap and original Domain grant.
use super::*;
use zero_dns::OwnedDnsQueryObservation;

async fn query(
    proxy: &Proxy,
    node: &mut crate::dns_fixture::Dns,
    server: &str,
    name: &str,
) -> OwnedDnsQueryObservation {
    query_with_addresses(
        proxy,
        node,
        server,
        name,
        &[IpAddress::V4([127, 0, 0, 2]), IpAddress::V4([127, 0, 0, 1])],
        2,
    )
    .await
}
async fn query_with_addresses(
    proxy: &Proxy,
    node: &mut crate::dns_fixture::Dns,
    server: &str,
    name: &str,
    addresses: &[IpAddress],
    original_ordinal: u64,
) -> OwnedDnsQueryObservation {
    let mut query = tokio::spawn({
        let dns = proxy.resolver.clone();
        let server = server.to_owned();
        let name = name.to_owned();
        async move { dns.query_ech_config(&name, &server).await }
    });
    let mut originals = Vec::new();
    loop {
        tokio::select! {
            result = &mut query => { assert_eq!(result.unwrap().unwrap().config_list, None); break; }
            request = node.request() => {
                let question = zero_dns::udp::parse_dns_question(&request.query).unwrap();
                assert!(!request.tcp); assert_eq!(question.query_type, 1); assert_eq!(question.domain, "endpoint.ech.test");
                let original = proxy.resolver.native_query_observations().into_iter().find(|record| !record.task.is_joined() && record.original.as_ref().and_then(|original| original.endpoints()).is_some_and(|endpoints| endpoints.peer() == node.address)).expect("actual Node bootstrap UDP original before its reply");
                originals.push(original);
                request.answer_ips(false, false, addresses);
            }
        }
    }
    assert!(
        !originals.is_empty(),
        "a domain endpoint cannot use a literal native grant without real Node bootstrap"
    );
    for original in &originals {
        assert!(
            original.task.is_joined(),
            "actual bootstrap Scope joined before the physical ECH connection"
        );
        let native = original.original.as_ref().unwrap();
        assert!(native
            .closed()
            .is_some_and(|closed| closed.matches(native) && closed.had_native_acquisition()));
    }
    let driver = proxy
        .resolver
        .native_query_observations()
        .into_iter()
        .find(|record| {
            !record.task.is_joined()
                && record.original.as_ref().is_some_and(|original| {
                    original.selected_binding().is_some_and(|(endpoint, _)| {
                        endpoint.address == zero_core::Address::Domain("endpoint.ech.test".into())
                    })
                })
        })
        .expect(
            "actual physical driver retains the original Domain, not a substituted literal grant",
        );
    assert!(originals
        .iter()
        .all(|query| !query.task.same_registration(&driver.task)));
    let original = driver.original.as_ref().unwrap();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let actual = plan.component(original.instance().component).unwrap();
    let owner = actual.recipe.spec().resources.iter().enumerate().filter_map(|(ordinal,resource)| {
        matches!(resource, zero_platform_tokio::contract::component::ResourceDeclaration::ControlledCarrierPool { endpoint, datagram:false, .. } if endpoint.address == zero_core::Address::Domain("endpoint.ech.test".into())).then(|| actual.original_carrier_owner(ordinal)).flatten()
    }).find(|owner| owner.observe().is_some_and(|held| held.retained() == 1)).expect("same actual Domain issuer only retains the successful original");
    assert_eq!(
        owner.observe().unwrap().slots()[0].ordinal(),
        original_ordinal,
        "same actual successful candidate original ordinal"
    );
    driver
}

#[tokio::test]
async fn actual_domain_ech_joins_node_bootstrap_retries_candidates_and_replaces_its_source() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut peer = peer::Peer::cleartext().await;
        let refused = tokio::net::TcpSocket::new_v4().unwrap();
        refused
            .bind(std::net::SocketAddr::from((
                [127, 0, 0, 2],
                peer.endpoint.port(),
            )))
            .unwrap();
        let endpoint = format!("h2c://endpoint.ech.test:{}/dns-query", peer.endpoint.port());
        let mut first = crate::dns_fixture::Dns::new().await;
        let mut second = crate::dns_fixture::Dns::new().await;
        let mut config = config(&endpoint);
        config.runtime.native_dns_max_queries = None;
        config.runtime.native_dns_max_originals = Some(2);
        config.runtime.native_dns_max_h2_drivers = Some(1);
        let mut dns = first.config();
        dns.policy.timeout_ms = 3000;
        config.runtime.dns = Some(dns);
        let proxy = Proxy::new(config.clone()).unwrap();
        let running = proxy.spawn();
        let handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(3))
            .await
            .unwrap();
        let original = query(&proxy, &mut first, &endpoint, "first.domain.ech.test").await;
        assert_eq!(peer.requests.recv().await.unwrap(), "first.domain.ech.test");
        assert_eq!(peer.accepted.load(Ordering::Acquire), 1);
        let native = original.original.as_ref().unwrap();
        for (index, addresses) in [
            vec![IpAddress::V4([127, 0, 0, 1])],
            vec![IpAddress::V4([127, 0, 0, 2]), IpAddress::V4([127, 0, 0, 1])],
            vec![IpAddress::V4([127, 0, 0, 1])],
        ]
        .into_iter()
        .enumerate()
        {
            let name = format!("rotate-{index}.domain.ech.test");
            let same =
                query_with_addresses(&proxy, &mut first, &endpoint, &name, &addresses, 2).await;
            assert!(
                same.task.same_registration(&original.task),
                "rotating actual Node candidates cannot create another endpoint pool or driver"
            );
            assert!(same.original.as_ref().unwrap().same_original(native));
            assert_eq!(peer.requests.recv().await.unwrap(), name);
            assert_eq!(peer.accepted.load(Ordering::Acquire), 1);
        }
        let mut dns = second.config();
        dns.policy.timeout_ms = 3000;
        config.runtime.dns = Some(dns);
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(3))
            .await
            .unwrap();
        assert!(original.task.is_joined());
        assert!(native
            .closed()
            .is_some_and(|closed| closed.matches(native) && closed.had_native_acquisition()));
        let successor = query(&proxy, &mut second, &endpoint, "next.domain.ech.test").await;
        assert_eq!(peer.requests.recv().await.unwrap(), "next.domain.ech.test");
        assert!(!successor.task.same_registration(&original.task));
        assert_eq!(peer.accepted.load(Ordering::Acquire), 2);
        first.no_new_request();
        running.shutdown().await.unwrap();
        assert!(successor.task.is_joined());
        let successor_native = successor.original.as_ref().unwrap();
        assert!(successor_native.closed().is_some_and(
            |closed| closed.matches(successor_native) && closed.had_native_acquisition()
        ));
        peer.finish().await;
        first.close().await;
        second.close().await;
        drop(refused);
    })
    .await
    .unwrap();
}

mod candidates;
