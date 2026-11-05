use super::*;

#[tokio::test]
async fn actual_doh_pool_network_change_confirms_old_original_before_new_physical_connection() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut tls = TlsDns::new(true).await;
        let plain = Dns::new().await;
        let mut dns = plain.config();
        dns.servers.insert("native".into(), tls.server(true));
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns);
        let (proxy, running, _) = start(config).await;
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "native", tls.certificate.clone())
            .unwrap();
        let first = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move { resolver.resolve_node("old.pool.node.test").await }
        });
        let (_, old_reply) = tokio::time::timeout(Duration::from_secs(4), tls.requests.recv())
            .await
            .unwrap()
            .unwrap();
        let old = pending(&proxy).await;
        let prior = original(&old);
        assert!(prior.closed().is_none());
        proxy.egress_interface_control().invalidate_network();
        let next = tokio::spawn({
            let resolver = proxy.resolver.clone();
            async move { resolver.resolve_node("new.pool.node.test").await }
        });
        let (query, reply) = tokio::time::timeout(Duration::from_secs(4), tls.requests.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(old.task.is_joined());
        closed(&prior);
        let fresh = original(&pending(&proxy).await);
        assert!(!fresh.same_original(&prior));
        assert!(first.await.unwrap().is_err());
        reply
            .send(zero_dns::udp::build_dns_response(
                &query,
                &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
            ))
            .unwrap();
        next.await.unwrap().unwrap();
        drop(old_reply);
        running.shutdown().await.unwrap();
        closed(&fresh);
        tls.stop.cancel();
        tls.task.await.unwrap();
        plain.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_doh_pool_noop_keeps_original_and_dns_owner_replacement_closes_before_reopening() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let mut tls = TlsDns::new(true).await;
        let plain = Dns::new().await;
        let mut dns = plain.config();
        dns.servers.insert("native".into(), tls.server(true));
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns);
        let (proxy, running, handle) = start(config.clone()).await;
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "native", tls.certificate.clone())
            .unwrap();
        let old = answer(&proxy, &mut tls, "first.pool.test").await;
        let prior = original(&old);
        handle
            .apply_runtime_config_and_wait(config.clone(), Duration::from_secs(5))
            .await
            .unwrap();
        let unchanged = answer(&proxy, &mut tls, "unchanged.pool.test").await;
        assert!(old.task.same_registration(&unchanged.task));
        assert!(prior.same_original(&original(&unchanged)));
        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(prior.closed().is_none());
        config.runtime.dns.as_mut().unwrap().policy.timeout_ms += 1;
        handle
            .apply_runtime_config_and_wait(config, Duration::from_secs(5))
            .await
            .unwrap();
        assert!(old.task.is_joined());
        closed(&prior);
        assert!(prior.owner().closed().unwrap().matches(prior.owner()));
        zero_dns::test_support::install_doh_ca(&proxy.resolver, "native", tls.certificate.clone())
            .unwrap();
        let fresh = answer(&proxy, &mut tls, "replaced.pool.test").await;
        assert!(!old.task.same_registration(&fresh.task));
        let next = original(&fresh);
        assert!(!prior.same_original(&next));
        assert_eq!(tls.connections.load(std::sync::atomic::Ordering::SeqCst), 2);
        running.shutdown().await.unwrap();
        assert!(fresh.task.is_joined());
        closed(&next);
        tls.stop.cancel();
        tls.task.await.unwrap();
        plain.close().await;
    })
    .await
    .unwrap();
}
async fn answer(proxy: &Proxy, tls: &mut TlsDns, name: &str) -> zero_dns::OwnedDnsQueryObservation {
    let job = tokio::spawn({
        let resolver = proxy.resolver.clone();
        let name = name.to_owned();
        async move { resolver.resolve_node(&name).await }
    });
    let (query, reply) = tokio::time::timeout(Duration::from_secs(4), tls.requests.recv())
        .await
        .unwrap()
        .unwrap();
    let held = pending(proxy).await;
    reply
        .send(zero_dns::udp::build_dns_response(
            &query,
            &[zero_traits::IpAddress::V4([127, 0, 0, 1])],
        ))
        .unwrap();
    job.await.unwrap().unwrap();
    held
}

#[tokio::test]
async fn ordinary_proxy_doh_uses_the_same_real_host_pool_without_background_protocol_admission() {
    exchange_in(true, false, false).await;
}
