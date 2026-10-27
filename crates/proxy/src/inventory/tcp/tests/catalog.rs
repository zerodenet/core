#![cfg(feature = "mieru")]
use super::*;

#[tokio::test]
async fn mixed_factory_only_stream_suffix_needs_no_tcp_claim_and_keeps_the_legacy_pool_prefix() {
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
                {"tag":"last","protocol":{"type":"socks5","server":"127.0.0.1","port":ports[2]}}
            ],
            "outbound_groups":[{"tag":"relay","type":"relay","proxies":members}],
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
                .is_none(),
            "ordinary Mieru native registration remains closed"
        );
        let (resolved, _) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("relay".into()))
            .unwrap();
        let PreparedTcpOutbound::Relay(prepared) = services
            .prepare_tcp_outbound(&resolved, RouteMode::Flow)
            .unwrap_or_else(|failure| panic!("{}", failure.error))
        else {
            panic!("actual mixed relay required")
        };
        assert!(matches!(
            prepared.first.execution,
            PreparedTcpCandidateExecution::Connect(_)
        ));
        if retain_prefix {
            assert!(
                matches!(
                    prepared.relay_hops[0].execution,
                    PreparedTcpRelayExecution::Legacy(_)
                ),
                "legacy pool retains its entire existing carrier prefix"
            );
        }
        let PreparedTcpRelayExecution::Native(selected) =
            &prepared.relay_hops.last().unwrap().execution
        else {
            panic!("factory-only final Stream fell back to a legacy TCP claim")
        };
        assert_eq!(
            selected.instance(),
            execution
                .components
                .as_ref()
                .unwrap()
                .configured("last")
                .unwrap()
                .instance_id()
        );
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
