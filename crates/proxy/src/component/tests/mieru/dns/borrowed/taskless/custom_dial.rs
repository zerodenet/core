//! A nondefault single leaf borrows its real policy-bound taskless original.
use super::*;

#[derive(Debug)]
struct NoLegacy;
impl zero_dns::DnsOutboundConnector for NoLegacy {
    fn connect(&self, _: String, _: std::net::SocketAddr) -> zero_dns::DnsOutboundConnectFuture {
        Box::pin(async { panic!("qualified custom-dial DNS must use its actual host original") })
    }
}

#[tokio::test]
async fn ordinary_taskless_single_leaf_dns_retains_its_actual_custom_dial_original_until_join() {
    tokio::time::timeout(Duration::from_secs(45), async {
        for protocol in ["vless", "vmess", "trojan", #[cfg(feature = "shadowsocks")] "shadowsocks"] {
            let mut prefix = prefix(protocol).await;
            prefix.node.dial = serde_json::from_value(serde_json::json!({
                "source_ip": "127.0.0.2", "address_family": "only_ipv4"
            })).unwrap();
            let mut dns = Dns::new().await;
            let mut config = config(["127.0.0.1:1".parse().unwrap(); 3]);
            // The configured consumer is the ordinary leaf itself. No relay,
            // MUX or resourceful pool can inherit this qualification.
            config.outbounds = vec![prefix.node.clone()];
            config.outbound_groups.clear();
            let mut settings = dns.config();
            let mut server = dns.server();
            let zero_config::DnsServerConfig::Udp { detour, .. } = &mut server else { unreachable!() };
            *detour = Some("prefix".into());
            settings.servers.insert("query".into(), server);
            settings.default_server = "query".into();
            settings.policy.node_server = Some("native".into());
            config.runtime.dns = Some(settings);
            let (proxy, running, _) = start(config).await;
            proxy.resolver.set_outbound_connector(Arc::new(NoLegacy));
            let mut originals = Vec::new();
            for (number, cancel) in [false, true].into_iter().enumerate() {
                let resolver = proxy.resolver.clone();
                let mut query = tokio::spawn(async move {
                    resolver.resolve_real_type(&format!("{protocol}-{number}.custom-dial.test"), 1).await
                });
                let request = tokio::select! {
                    request = dns.request() => request,
                    result = &mut query => panic!("{protocol} custom-dial DNS failed before actual IO: {result:?}"),
                };
                assert!(request.tcp);
                let caller = pending(&proxy).await;
                let child = caller.original.clone().unwrap();
                let original = child.parent().expect("actual taskless donor").clone();
                assert!(original.driver().is_none());
                assert!(original.parent().is_none());
                let endpoints = original.endpoints().expect("same actual connected socket");
                assert_eq!(endpoints.local().unwrap().ip(), "127.0.0.2".parse::<std::net::IpAddr>().unwrap());
                assert!(endpoints.peer().is_ipv4());
                assert!(original.closed().is_none());
                assert!(!caller.task.is_joined());
                if cancel {
                    query.abort();
                    assert!(query.await.unwrap_err().is_cancelled());
                    drop(request);
                } else {
                    request.answer(false, false);
                    query.await.unwrap().unwrap();
                }
                tokio::time::timeout_at(deadline(), caller.task.wait()).await.unwrap();
                let returned = child.closed().expect("same DNS logical receiver close");
                assert!(returned.matches(&child) && !returned.had_native_acquisition());
                let closed = original.closed().expect("same actual custom-dial native close");
                assert!(closed.matches(&original) && closed.had_native_acquisition());
                if let Some(previous) = originals.last() {
                    assert!(!original.same_original(previous));
                }
                originals.push(original);
            }
            assert!(proxy.engine().active_sessions().is_empty());
            assert!(proxy.engine().completed_sessions().is_empty());
            running.shutdown().await.unwrap();
            for original in originals {
                assert!(original.owner().closed().unwrap().matches(original.owner()));
                assert!(original.owner().users_closed().unwrap().matches(original.owner()));
            }
            dns.close().await;
            prefix.close().await;
        }
    }).await.unwrap();
}
