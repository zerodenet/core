#![cfg(all(feature = "mieru", feature = "vless"))]
use super::*;

#[tokio::test]
async fn factory_only_stream_suffix_keeps_qualified_native_pools_with_unrelated_mkcp_aliases() {
    let mut listeners = Vec::new();
    for _ in 0..3 {
        listeners.push(tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap());
    }
    let ports: Vec<_> = listeners
        .iter()
        .map(|listener| listener.local_addr().unwrap().port())
        .collect();
    for retain_prefix in [false, true] {
        let members = if retain_prefix {
            vec!["first", "pooled", "last"]
        } else {
            vec!["pooled", "last"]
        };
        let config = serde_json::json!({
            "outbounds":[
                {"tag":"first","protocol":{"type":"socks5","server":"127.0.0.1","port":ports[0]}},
                {"tag":"pooled","protocol":{"type":"mieru","server":"127.0.0.1","port":ports[1],"username":"test-user","password":"test-password"}},
                {"tag":"last","protocol":{"type":"socks5","server":"127.0.0.1","port":ports[2]}},
                {"tag":"unmigrated","protocol":{"type":"vless","server":"127.0.0.1","port":ports[0],"id":"11111111-2222-3333-4444-555555555555","mkcp":{}}}
            ],
            "outbound_groups":[
                {"tag":"relay","type":"relay","proxies":members.clone()},
                {"tag":"unmigrated-relay","type":"relay","proxies":["unmigrated","pooled"]}
            ],
            "route":{"final":{"type":"route","outbound":"relay"}}
        });
        let mut proxy =
            Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
        if !retain_prefix {
            // Remove only the compatibility projection. Keep every actual
            // adapter, protocol pool and host factory from the same assembly.
            proxy.protocols.registry = std::sync::Arc::new(
                proxy
                    .protocols
                    .registry
                    .as_ref()
                    .clone()
                    .without_outbound_projection("socks5"),
            );
        }
        let mut session = zero_core::Session::new(
            0,
            zero_core::Address::Ipv4([203, 0, 113, 9]),
            443,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("test"),
        );
        proxy
            .engine()
            .prepare_session(&mut session, "incoming")
            .unwrap();
        let services = proxy
            .tcp_runtime_services()
            .for_inbound("incoming".into())
            .with_business_identity(&session)
            .unwrap();
        let execution = services.execution();
        assert!(
            execution
                .components
                .as_ref()
                .unwrap()
                .configured("pooled")
                .is_some(),
            "an unrelated alias cannot demote the adapter's actual pool"
        );
        let (resolved, _) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("relay".into()))
            .unwrap();
        let PreparedTcpOutbound::NativeRelay(selected) = services
            .prepare_tcp_outbound(&resolved, RouteMode::Flow)
            .unwrap_or_else(|failure| panic!("{}", failure.error))
        else {
            panic!("actual standard chain required");
        };
        assert_eq!(selected.nodes.len(), members.len());
        for node in &selected.nodes {
            assert_eq!(
                node.slot.instance(),
                execution
                    .components
                    .as_ref()
                    .unwrap()
                    .configured(&node.tag)
                    .unwrap()
                    .instance_id()
            );
        }
        for listener in &listeners {
            let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
            assert!(
                listener.poll_accept(&mut cx).is_pending(),
                "cold preparation must not acquire carriers"
            );
        }
        proxy
            .protocols
            .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(2))
            .await
            .unwrap();
    }
}
