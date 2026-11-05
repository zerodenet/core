//! Ordinary taskless originals feed the same real Mieru DNS pool.
use super::*;
mod custom_dial;

async fn prefix(protocol: &str) -> crate::runtime::MuxPrefix {
    let mut prefix = if protocol == "shadowsocks" {
        crate::runtime::MuxPrefix::shadowsocks("aes-128-gcm", "prefix").await
    } else {
        crate::runtime::MuxPrefix::start(protocol, "prefix").await
    };
    match &mut prefix.node.protocol {
        zero_config::OutboundProtocolConfig::Vless {
            mux_concurrency,
            xudp_concurrency,
            ..
        } => {
            *mux_concurrency = None;
            *xudp_concurrency = None;
        }
        zero_config::OutboundProtocolConfig::Vmess {
            mux_concurrency, ..
        }
        | zero_config::OutboundProtocolConfig::Trojan {
            mux_concurrency, ..
        } => *mux_concurrency = None,
        _ => {}
    }
    prefix
}

#[tokio::test]
async fn ordinary_taskless_dns_prefixes_return_only_query_loans_and_reuse_actual_pool_originals() {
    tokio::time::timeout(Duration::from_secs(45), async {
        for protocol in ["vless", "vmess", "trojan", #[cfg(feature = "shadowsocks")] "shadowsocks"] {
            let prefix = prefix(protocol).await;
            let mut dns = Dns::new().await;
            let server = Server::new(true).await;
            let mut config = detoured(&dns, &server);
            config.outbounds.push(prefix.node.clone());
            config.outbound_groups = zero_config::RuntimeConfig::parse(r#"{"outbound_groups":[{"tag":"query-chain","type":"relay","proxies":["prefix","a"]}],"outbounds":[{"tag":"prefix","protocol":{"type":"direct"}},{"tag":"a","protocol":{"type":"direct"}}],"route":{"final":{"type":"direct"}}}"#).unwrap().outbound_groups;
            let zero_config::DnsServerConfig::Udp { detour, .. } = config.runtime.dns.as_mut().unwrap().servers.get_mut("query").unwrap() else { unreachable!() };
            *detour = Some("query-chain".into());
            let (proxy, running, _) = start(config).await;
            let mut prior = None;
            for (number, cancel) in [false, true, false].into_iter().enumerate() {
                let resolver = proxy.resolver.clone();
                let mut query = tokio::spawn(async move { resolver.resolve_real_type(&format!("{protocol}-{number}.loan.test"), 1).await });
                let request = tokio::select! { request = dns.request() => request, result = &mut query => panic!("{protocol} DNS failed before actual fixture IO: {result:?}") };
                assert!(request.tcp);
                let query_original = pending(&proxy).await;
                let child = query_original.original.clone().unwrap();
                let pool = child.parent().expect("same actual Mieru donor").clone();
                let root = pool.parent().expect("actual taskless donor").clone();
                assert!(root.driver().is_none());
                assert!(root.parent().is_none());
                assert!(root.closed().is_none());
                assert!(!pool.driver().unwrap().is_joined());
                if let Some((old_pool, old_root)) = &prior {
                    assert!(pool.same_original(old_pool));
                    assert!(root.same_original(old_root));
                }
                if cancel { query.abort(); assert!(query.await.unwrap_err().is_cancelled()); drop(request); }
                else { request.answer(false, false); query.await.unwrap().unwrap(); }
                tokio::time::timeout_at(deadline(), query_original.task.wait()).await.unwrap();
                assert!(child.closed().unwrap().matches(&child));
                assert!(!child.closed().unwrap().had_native_acquisition());
                assert!(pool.closed().is_none());
                assert!(root.closed().is_none());
                assert!(!pool.driver().unwrap().is_joined());
                assert_eq!(server.accepts.load(Ordering::SeqCst), 1);
                prior = Some((pool, root));
            }
            assert!(proxy.engine().active_sessions().is_empty());
            assert!(proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            let (pool, root) = prior.unwrap();
            assert!(pool.driver().unwrap().is_joined());
            assert!(root.closed().unwrap().matches(&root));
            assert!(root.closed().unwrap().had_native_acquisition());
            server.close().await;
            dns.close().await;
            prefix.close().await;
        }
    }).await.unwrap();
}

#[tokio::test]
async fn cancelled_dns_wire_prepare_closes_the_same_actual_original_without_a_later_dial() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let mut dns = Dns::new().await;
        let mut config = config([address; 3]);
        config.outbounds.truncate(1);
        config.outbound_groups.clear();
        let mut settings = dns.config();
        let mut detour = dns.server();
        let zero_config::DnsServerConfig::Udp { detour: target, .. } = &mut detour else { unreachable!() };
        *target = Some("a".into());
        settings.servers.insert("query".into(), detour);
        settings.default_server = "query".into();
        settings.policy.node_server = Some("native".into());
        config.runtime.dns = Some(settings);
        let (proxy, running, _) = start(config).await;
        let resolver = proxy.resolver.clone();
        let mut query = tokio::spawn(async move { resolver.resolve_real_type("wire.pending.test", 1).await });
        let (mut socket, _) = tokio::select! { result = listener.accept() => result.unwrap(), result = &mut query => panic!("DNS never reached actual wire preparation: {result:?}") };
        let mut first_byte = [0u8; 1];
        socket.read_exact(&mut first_byte).await.unwrap();
        let observation = proxy.resolver.native_query_observations().into_iter().find(|query| !query.task.is_joined()).expect("actual DNS caller is suspended before loan handoff");
        let services = proxy.tcp_runtime_services();
        let plan = services.components.as_ref().unwrap();
        let role = plan.configured("a").unwrap();
        let pool = role.dependency_instances().iter().find_map(|instance| plan.original_carrier_owner(*instance, 0)).expect("same actual Mieru pool original");
        let held = pool.observe().unwrap();
        assert_eq!(held.retained(), 1);
        let original = held.slots()[0].original().clone();
        assert!(original.closed().is_none());
        query.abort();
        assert!(query.await.unwrap_err().is_cancelled());
        tokio::time::timeout_at(deadline(), observation.task.wait()).await.unwrap();
        let mut tail = Vec::new();
        tokio::time::timeout_at(deadline(), socket.read_to_end(&mut tail)).await.unwrap().unwrap();
        let closed = original.closed().expect("pre-handoff cancellation actually consumed the same native original");
        assert!(original.matches_closed(&closed));
        assert_eq!(pool.observe().unwrap().retained(), 0, "no later dial may be needed to collect the cancelled original");
        dns.no_new_request();
        running.shutdown().await.unwrap();
        dns.close().await;
    }).await.unwrap();
}
