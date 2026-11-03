use super::*;
#[tokio::test]
async fn ordinary_vmess_grpc_domain_dns_apply_retires_transitive_consumer_before_replacement_provider_starts(
) {
    timeout(Duration::from_secs(25), async {
        let mut dns = Dns::new().await;
        let mut next = Dns::new().await;
        let endpoint = Endpoint::new().await;
        let config = endpoint.config(&dns, true);
        let (proxy, running, handle) = start_domain(config.clone()).await;
        let server = endpoint.serve(true, 2, vec![b"owned".to_vec()]);
        let opening = tokio::spawn(open(proxy.clone(), 1, b"owned"));
        let request = dns.request().await;
        let prior = pending(&proxy).await;
        request.answer(false, false);
        let dispatch = opening.await.unwrap();
        prior.task.wait().await;
        dns_closed(&prior);
        let registration = original(&proxy);
        let observed = registration.observe().unwrap();
        let native = observed.slots()[0].original().clone();
        let driver = observed.slots()[0].driver().unwrap().1.clone();
        let old_plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let old = old_plan.configured("node").unwrap();
        let historical = old
            .datagram(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        let mut candidate = config;
        candidate.runtime.dns = Some(next.config());
        let replacing = tokio::spawn(async move {
            handle
                .apply_runtime_config_and_wait(candidate, Duration::from_secs(10))
                .await
        });
        driver.wait().await;
        assert!(!replacing.is_finished());
        assert!(registration.closed().is_none());
        assert!(
            prior.original.as_ref().unwrap().owner().closed().is_none(),
            "incomplete consumer blocks dependency cleanup"
        );
        next.no_new_request();
        retire(&proxy, dispatch).await;
        replacing.await.unwrap().unwrap();
        assert!(native.matches_closed(&native.closed().unwrap()));
        assert!(registration.closed().unwrap().matches(&registration));
        assert!(prior
            .original
            .as_ref()
            .unwrap()
            .owner()
            .closed()
            .unwrap()
            .matches(prior.original.as_ref().unwrap().owner()));
        let new_plan = proxy.tcp_runtime_services().components.clone().unwrap();
        let new = new_plan.configured("node").unwrap();
        assert_ne!(old.instance_id(), new.instance_id());
        assert!(old
            .datagram(Instant::now() + Duration::from_secs(3))
            .await
            .is_err());
        assert_eq!(original(&proxy).observe().unwrap().retained(), 0);
        let opening = tokio::spawn(open(proxy.clone(), 2, b"owned"));
        let request = next.request().await;
        let current = pending(&proxy).await;
        assert_ne!(
            prior.original.as_ref().unwrap().instance(),
            current.original.as_ref().unwrap().instance()
        );
        assert!(!prior
            .task
            .scope_identity()
            .same_scope(&current.task.scope_identity()));
        request.answer(false, false);
        let dispatch = opening.await.unwrap();
        current.task.wait().await;
        dns_closed(&current);
        retire(&proxy, dispatch).await;
        drop(historical);
        server.await.unwrap();
        running.shutdown().await.unwrap();
        dns.close().await;
        next.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn ordinary_vmess_grpc_domain_route_only_apply_reuses_owner_and_keeps_live_udp_while_opening_fresh_flow(
) {
    timeout(Duration::from_secs(20), async {
        let mut dns = Dns::new().await;
        let endpoint = Endpoint::new().await;
        let config = endpoint.config(&dns, false);
        let (proxy, running, handle) = start_domain(config.clone()).await;
        let server = endpoint.serve(false, 2, vec![b"owned".to_vec()]);
        let opening = tokio::spawn(open(proxy.clone(), 1, b"owned"));
        dns.request().await.answer(false, false);
        let first = opening.await.unwrap();
        let before = proxy
            .tcp_runtime_services()
            .components
            .clone()
            .unwrap()
            .configured("node")
            .unwrap()
            .instance_id();
        let registration = original(&proxy);
        let old = registration.observe().unwrap().slots()[0]
            .original()
            .clone();
        let mut candidate = config;
        candidate.outbound_groups = serde_json::from_value(
            serde_json::json!([{"tag":"choice","type":"selector","outbounds":["node"]}]),
        )
        .unwrap();
        candidate.route = serde_json::from_value(
            serde_json::json!({"final":{"type":"route","outbound":"choice"}}),
        )
        .unwrap();
        handle
            .apply_runtime_config_and_wait(candidate, Duration::from_secs(5))
            .await
            .unwrap();
        let after = proxy
            .tcp_runtime_services()
            .components
            .clone()
            .unwrap()
            .configured("node")
            .unwrap()
            .instance_id();
        assert_eq!(
            before, after,
            "unchanged DNS and node retain their actual role owner"
        );
        assert!(old.closed().is_none());
        let opening = tokio::spawn(open(proxy.clone(), 2, b"owned"));
        dns.request().await.answer(false, false);
        let second = opening.await.unwrap();
        assert!(
            old.closed().is_none(),
            "resolver publication alone must not force-drain unchanged live UDP"
        );
        assert_eq!(registration.observe().unwrap().retained(), 2);
        // Retire both views before waiting on the shared retirement queue.
        first.finish_all();
        retire(&proxy, second).await;
        assert!(old.matches_closed(&old.closed().unwrap()));
        server.await.unwrap();
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}
