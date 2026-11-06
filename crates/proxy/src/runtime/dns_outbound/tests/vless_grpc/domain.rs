//! Domain carriers use the original Node service before the real gRPC pool.
use super::*;
async fn delivered(
    proxy: &Proxy,
    node: &mut crate::dns_fixture::Dns,
    dns: &mut crate::dns_fixture::Dns,
    name: &str,
) -> CarrierOrigin {
    let mut query = query(proxy, name);
    let mut bootstrap = Vec::new();
    let request = loop {
        tokio::select! {
            request = node.request() => {
                let question = zero_dns::udp::parse_dns_question(&request.query).unwrap();
                assert_eq!(question.domain,"grpc.actual.node.test"); assert_eq!(question.query_type,1);
                assert!(!request.tcp);
                let original = proxy.resolver.native_query_observations().into_iter().find(|record| record.original.as_ref().and_then(|original| original.endpoints()).is_some_and(|endpoints| endpoints.peer() == node.address)).expect("actual Node bootstrap original");
                bootstrap.push(original);
                request.answer_ips(false,false,&[IpAddress::V4([127,0,0,2]),IpAddress::V4([127,0,0,1])]);
            }
            request = dns.request() => break request,
            result = &mut query => panic!("domain VLESS gRPC failed before actual DNS delivery: {result:?}"),
        }
    };
    assert_eq!(
        bootstrap.len(),
        1,
        "one actual Service invocation resolves Node once before candidate retry"
    );
    for original in bootstrap {
        tokio::time::timeout_at(deadline(), original.task.wait())
            .await
            .unwrap()
            .result()
            .unwrap();
        let native = original.original.as_ref().unwrap();
        assert!(native
            .closed()
            .is_some_and(|closed| closed.matches(native) && closed.had_native_acquisition()));
    }
    assert!(request.tcp);
    let caller = pending(proxy).await;
    let child = caller.original.as_ref().unwrap();
    let donor = child.parent().unwrap().clone();
    let plan = proxy.tcp_runtime_services().components.clone().unwrap();
    let slot = plan.component(donor.instance().component).unwrap();
    let original = slot
        .recipe
        .spec()
        .resources
        .iter()
        .enumerate()
        .filter_map(|(ordinal, _)| slot.original_carrier_owner(ordinal))
        .find_map(|owner| {
            owner.observe().and_then(|held| {
                held.slots()
                    .iter()
                    .find(|entry| {
                        entry.driver().is_some_and(|(_, driver)| {
                            driver.same_registration(donor.driver().unwrap())
                        })
                    })
                    .map(|entry| (held.retained(), entry.ordinal()))
            })
        })
        .expect("same actual canonical pool's successful native original");
    assert_eq!(original, (1, 2), "failed original must have returned to the actual provider before the second candidate survives");
    assert_eq!(
        donor.selected_binding().unwrap().0.address,
        zero_core::Address::Domain("grpc.actual.node.test".into())
    );
    request.answer(false, false);
    assert_eq!(
        query.await.unwrap().unwrap(),
        vec![IpAddress::V4([127, 0, 0, 1])]
    );
    tokio::time::timeout_at(deadline(), caller.task.wait())
        .await
        .unwrap()
        .result()
        .unwrap();
    returned(child);
    assert!(donor.closed().is_none() && !donor.driver().unwrap().is_joined());
    donor
}
#[tokio::test]
async fn ordinary_domain_vless_grpc_dns_replaces_the_actual_node_dependency_before_its_next_pool() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let remote = Remote::new(false).await;
        let refused = tokio::net::TcpSocket::new_v4().unwrap();
        refused
            .bind(std::net::SocketAddr::from((
                [127, 0, 0, 2],
                remote.address.port(),
            )))
            .unwrap();
        let mut dns = crate::dns_fixture::Dns::new().await;
        let mut node = crate::dns_fixture::Dns::new().await;
        let mut next_node = crate::dns_fixture::Dns::new().await;
        let (proxy, running) = start_with_node(remote.address, false, &dns, Some(&node)).await;
        let old = delivered(&proxy, &mut node, &mut dns, "first.domain.vless.grpc.test").await;
        let mut candidate = (*proxy.engine().config()).clone();
        candidate
            .runtime
            .dns
            .as_mut()
            .unwrap()
            .servers
            .insert("native".into(), next_node.server());
        let handle = crate::ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        );
        handle
            .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
            .await
            .unwrap();
        native_closed(&old);
        let fresh = delivered(
            &proxy,
            &mut next_node,
            &mut dns,
            "next.domain.vless.grpc.test",
        )
        .await;
        assert!(!fresh.same_original(&old));
        assert!(!fresh
            .driver()
            .unwrap()
            .same_registration(old.driver().unwrap()));
        node.no_new_request();
        assert!(
            proxy.engine().active_sessions().is_empty()
                && proxy.engine().completed_sessions().is_empty()
        );
        running.shutdown().await.unwrap();
        native_closed(&fresh);
        remote.close().await;
        dns.close().await;
        node.close().await;
        next_node.close().await;
        drop(refused);
    })
    .await
    .unwrap();
}
