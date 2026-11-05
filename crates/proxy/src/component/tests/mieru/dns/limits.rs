use super::*;

#[tokio::test]
async fn actual_proxy_native_dns_capacity_is_bounded_by_the_one_actual_host_query_scope() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        config.runtime.dns = Some(dns.config());
        let (proxy, running, _) = start(config).await;
        let mut jobs = Vec::new();
        let mut requests = Vec::new();
        for number in 0..crate::component::native_dns::LIMIT {
            let resolver = proxy.resolver.clone();
            jobs.push(tokio::spawn(async move {
                resolver
                    .resolve_node(&format!("bounded-{number}.node.test"))
                    .await
            }));
            requests.push(dns.request().await);
        }
        let held = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let observations = proxy.resolver.native_query_observations();
                if observations.len() == crate::component::native_dns::LIMIT
                    && observations.iter().all(|query| {
                        query.io_pending && query.original.is_some() && !query.task.is_joined()
                    })
                {
                    break observations;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let error = proxy
            .resolver
            .resolve_node("capacity-exhausted.node.test")
            .await
            .unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
        dns.no_new_request();
        for query in &held {
            assert!(query
                .task
                .scope_identity()
                .same_scope(&held[0].task.scope_identity()));
            assert_eq!(
                original(query).owner().limit().get(),
                crate::component::native_dns::LIMIT
            );
        }
        running.shutdown().await.unwrap();
        for job in jobs {
            assert!(job.await.unwrap().is_err());
        }
        for query in held {
            assert!(query.task.is_joined());
            let prior = original(&query);
            closed(&prior);
            assert!(prior.owner().closed().unwrap().matches(prior.owner()));
        }
        drop(requests);
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_native_dns_timeout_closes_the_original_before_a_later_query_opens() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut dns = Dns::new().await;
        let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
        let mut settings = dns.config();
        settings.policy.timeout_ms = 500;
        config.runtime.dns = Some(settings);
        let (proxy, running, _) = start(config).await;
        let job = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("timed-out.node.test").await })
        };
        let request = dns.request().await;
        let previous = pending(&proxy).await;
        let prior = original(&previous);
        assert_eq!(
            job.await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::TimedOut
        );
        previous.task.wait().await;
        closed(&prior);
        let job = {
            let resolver = proxy.resolver.clone();
            tokio::spawn(async move { resolver.resolve_node("after-timeout.node.test").await })
        };
        let next = dns.request().await;
        closed(&prior);
        next.answer(false, false);
        job.await.unwrap().unwrap();
        drop(request);
        running.shutdown().await.unwrap();
        dns.close().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn actual_proxy_native_dns_admits_owned_system_and_rejects_unowned_default_detour() {
    tokio::time::timeout(Duration::from_secs(20), async {
        for mode in 0..2 {
            let mut dns = Dns::new().await;
            let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
            let mut settings = dns.config();
            let server = match mode {
                0 => zero_config::DnsServerConfig::System,
                _ => zero_config::DnsServerConfig::Udp {
                    host: "127.0.0.1".into(),
                    port: dns.address.port(),
                    bootstrap: vec![],
                    detour: Some("a".into()),
                },
            };
            settings.servers.insert("native".into(), server);
            if mode == 1 {
                // A legal independent Node bootstrap prevents detour recursion.
                // The request below targets Default's detour, never bootstrap.
                settings.servers.insert("bootstrap".into(), dns.server());
                settings.policy.node_server = Some("bootstrap".into());
            }
            config.runtime.dns = Some(settings);
            let (proxy, running, _) = start(config).await;
            if mode == 0 {
                let addresses = proxy.resolver.resolve_node("127.0.0.1").await.unwrap();
                assert_eq!(addresses, vec![zero_traits::IpAddress::V4([127, 0, 0, 1])]);
            } else {
                let error = proxy
                    .resolver
                    .resolve_real("unsupported.detour.test")
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), std::io::ErrorKind::Unsupported, "{error}");
            }
            dns.no_new_request();
            assert!(proxy
                .resolver
                .native_query_observations()
                .iter()
                .all(|query| query.original.is_none()));
            running.shutdown().await.unwrap();
            dns.close().await;
        }
    })
    .await
    .unwrap();
}
