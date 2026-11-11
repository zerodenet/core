//! Real factory/catalog selection with the old projection physically absent.
use super::*;
use zero_platform_tokio::contract::component::RoleKind;

#[tokio::test]
async fn standard_outbound_cached_native_relay_releases_every_retired_actual_owner() {
    let (proxy, _session, services, listeners) = fixtures::setup(3).await;
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(RouteDecision::Route("relay".into()))
        .unwrap();
    let cached = services
        .prepare_tcp_outbound(&resolved, RouteMode::Flow)
        .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
    let PreparedTcpOutbound::NativeRelay(chain) = &cached else {
        panic!("actual installed Stream relay required")
    };
    let retired: Vec<_> = chain
        .nodes
        .iter()
        .map(|node| std::sync::Arc::downgrade(&node.slot.slot().unwrap()))
        .collect();
    for listener in &listeners {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(listener.poll_accept(&mut context).is_pending());
    }
    proxy
        .protocols
        .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
    drop(services);
    drop(proxy);
    assert!(
        retired.iter().all(|owner| owner.upgrade().is_none()),
        "a cached native relay pinned a retired protocol factory or pool"
    );
    let PreparedTcpOutbound::NativeRelay(chain) = cached else {
        unreachable!()
    };
    let expected = chain.nodes[0].slot.instance().component;
    let (next_proxy, session, next_services, next_listeners) = fixtures::setup(3).await;
    let failure = match crate::component::stream_bridge::connect_relay(
        &next_services.execution(),
        &session,
        chain,
        false,
    )
    .await
    {
        Ok(_) => panic!("an expired prepared owner selected a new generation"),
        Err(failure) => failure,
    };
    let source = crate::transport::channel_failure(&failure.error).unwrap();
    assert_eq!(
        source.reason,
        zero_platform_tokio::contract::Reason::Revoked
    );
    assert_eq!(source.component, Some(expected));
    assert_eq!(
        source.session,
        Some(zero_core::component::SessionId(session.id))
    );
    for listener in listeners.iter().chain(&next_listeners) {
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(listener.poll_accept(&mut context).is_pending());
    }
    next_proxy
        .protocols
        .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

#[cfg(feature = "socks5")]
#[tokio::test]
async fn standard_outbound_probe_endpoint_reads_public_facts_without_the_old_projection() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let config = zero_config::RuntimeConfig::parse(
        &serde_json::json!({
            "outbounds":[{"tag":"node","protocol":{
                "type":"socks5","server":"127.0.0.1","port":port
            }}],"route":{"final":{"type":"route","outbound":"node"}}
        })
        .to_string(),
    )
    .unwrap();
    let mut proxy = crate::Proxy::new(config).unwrap();
    proxy.protocols.registry = std::sync::Arc::new(
        proxy
            .protocols
            .registry
            .as_ref()
            .clone()
            .without_outbound_projection("socks5"),
    );
    let snapshot = proxy.engine().runtime_snapshot();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision_in_snapshot(&snapshot, RouteDecision::Route("node".into()))
        .unwrap();
    let ResolvedOutbound::Single(leaf) = resolved else {
        unreachable!()
    };
    assert_eq!(
        proxy.protocols.outbound_endpoint(&snapshot, &leaf).unwrap(),
        Some(("127.0.0.1".into(), port)),
    );
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(listener.poll_accept(&mut context).is_pending());
}

fn protocols() -> Vec<serde_json::Value> {
    #[allow(unused_mut)]
    let mut protocols = vec![serde_json::json!({"type":"socks5"})];
    #[cfg(feature = "mieru")]
    protocols.push(
        serde_json::json!({"type":"mieru", "username":"test-user", "password":"test-password"}),
    );
    #[cfg(feature = "hysteria2")]
    protocols
        .push(serde_json::json!({"type":"hysteria2", "password":"test-password", "insecure":true}));
    #[cfg(feature = "shadowsocks")]
    protocols.push(serde_json::json!({"type":"shadowsocks", "cipher":"2022-blake3-aes-128-gcm", "password":"AAAAAAAAAAAAAAAAAAAAAA=="}));
    #[cfg(feature = "trojan")]
    for mux in [false, true] {
        let mut protocol = serde_json::json!({"type":"trojan", "password":"test-password", "tls":{"insecure":true}});
        if mux {
            protocol["mux_concurrency"] = 4.into();
        }
        protocols.push(protocol);
    }
    #[cfg(feature = "vless")]
    for mux in [false, true] {
        let mut protocol = serde_json::json!({
            "type":"vless", "id":"11111111-2222-3333-4444-555555555555"
        });
        if mux {
            protocol["mux_concurrency"] = 4.into();
            protocol["xudp_concurrency"] = 4.into();
        }
        protocols.push(protocol);
    }
    #[cfg(feature = "vmess")]
    for mux in [false, true] {
        let mut protocol = serde_json::json!({
            "type":"vmess", "id":"11111111-2222-3333-4444-555555555555"
        });
        if mux {
            protocol["mux_concurrency"] = 4.into();
        }
        protocols.push(protocol);
    }
    protocols
}

#[tokio::test]
async fn standard_outbound_selection_needs_no_compatibility_claim_for_stream_datagram_or_packet_flow(
) {
    for mut protocol in protocols() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        protocol["server"] = "127.0.0.1".into();
        protocol["port"] = listener.local_addr().unwrap().port().into();
        let name = protocol["type"].as_str().unwrap().to_owned();
        let config = serde_json::json!({
            "outbounds":[{"tag":"node", "protocol":protocol}],
            "route":{"final":{"type":"route","outbound":"node"}}
        });
        let mut proxy =
            Proxy::new(zero_config::RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap();
        if matches!(
            name.as_str(),
            "vless" | "vmess" | "trojan" | "mieru" | "shadowsocks"
        ) {
            let snapshot = proxy.engine().runtime_snapshot();
            let (resolved, _) = proxy
                .engine()
                .resolve_route_decision_in_snapshot(&snapshot, RouteDecision::Route("node".into()))
                .unwrap();
            let ResolvedOutbound::Single(leaf) = resolved else {
                unreachable!()
            };
            let legacy = proxy
                .protocols
                .registry
                .claim_outbound_leaf(snapshot.config(), leaf);
            assert!(
                legacy.is_err(),
                "{name} reintroduced a migrated production executor"
            );
        }
        proxy.protocols.registry = std::sync::Arc::new(
            proxy
                .protocols
                .registry
                .as_ref()
                .clone()
                .without_outbound_projection(&name),
        );
        let config = proxy.engine().runtime_snapshot().config().clone();
        proxy
            .protocols
            .start_components(
                config.clone(),
                tokio::time::Instant::now() + Duration::from_secs(3),
            )
            .await
            .unwrap();
        proxy
            .protocols
            .commit_components(&config, || Ok(()))
            .unwrap();
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
            execution.invocation.is_some(),
            "the actual ingress must supply the business invocation"
        );
        let plan = execution.components.as_ref().unwrap();
        let slot = plan.configured("node").expect("actual standard factory");
        assert!(
            slot.recipe.spec().roles.contains(&RoleKind::Stream),
            "{protocol}"
        );
        let (resolved, _) = proxy
            .engine()
            .resolve_route_decision(RouteDecision::Route("node".into()))
            .unwrap();
        let cached = services
            .prepare_tcp_outbound(&resolved, RouteMode::Flow)
            .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
        assert!(matches!(
            &cached,
            PreparedTcpOutbound::Single(PreparedTcpCandidate {
                execution: PreparedTcpCandidateExecution::Native(_),
                ..
            })
        ));
        if !slot.recipe.spec().roles.contains(&RoleKind::Packet) {
            let failure = services
                .prepare_tcp_outbound(&resolved, RouteMode::Packet)
                .err()
                .expect("a standard Stream cannot become a Packet executor");
            assert_eq!(failure.stage, "data_plane_route");
            assert!(failure
                .error
                .to_string()
                .contains("no executable TCP path for route mode Packet"));
            #[cfg(feature = "udp-runtime")]
            if slot.recipe.spec().roles.contains(&RoleKind::Datagram) {
                let context = crate::protocol_registry::UdpAdapterContext::new(
                    services.config(),
                    crate::protocol_registry::UdpRuntimeServices::new(services.clone()),
                );
                let failure = proxy
                    .protocols
                    .prepare_udp_outbound(
                        context,
                        &session,
                        &resolved,
                        b"strict",
                        RouteMode::Packet,
                    )
                    .err()
                    .expect("a standard Datagram cannot become a Packet executor");
                assert_eq!(failure.stage, "data_plane_route");
                assert!(failure
                    .error
                    .to_string()
                    .contains("no executable UDP path for route mode Packet"));
            }
        }
        #[cfg(feature = "raw-ip-runtime")]
        for (ip_protocol, role) in [
            (zero_stack::packet::IPPROTO_TCP, RoleKind::Stream),
            (zero_stack::packet::IPPROTO_UDP, RoleKind::Datagram),
        ] {
            if !slot.recipe.spec().roles.contains(&role) {
                continue;
            }
            for mode in [RouteMode::Auto, RouteMode::Flow, RouteMode::Translate] {
                assert!(
                    matches!(
                        proxy.protocols.prepare_packet_route_target_admitted(
                            &config,
                            resolved.clone(),
                            Some(ip_protocol),
                            mode,
                            &zero_engine::EndpointAdmission::from_config(&config),
                            Some(plan),
                        ),
                        crate::inventory::PacketRouteTarget::Flow
                    ),
                    "{protocol}, {role:?}, {mode:?}"
                );
            }
            assert!(
                matches!(
                    proxy.protocols.prepare_packet_route_target_admitted(
                        &config,
                        resolved.clone(),
                        Some(ip_protocol),
                        RouteMode::Packet,
                        &zero_engine::EndpointAdmission::from_config(&config),
                        Some(plan),
                    ),
                    crate::inventory::PacketRouteTarget::Unsupported
                ),
                "Stream/Datagram cannot invent native Packet support"
            );
        }
        #[cfg(feature = "udp-runtime")]
        if slot.recipe.packet_path_facts().is_some()
            && slot.recipe.spec().roles.contains(&RoleKind::Datagram)
        {
            let ResolvedOutbound::Single(leaf) = &resolved else {
                unreachable!()
            };
            let path = proxy
                .protocols
                .optional_tcp_packet_path(OutboundAdapterContext::new(&config), leaf, Some(plan))
                .expect("same prepared factory supplies packet carrier facts");
            assert_eq!(
                path.carrier_descriptor().unwrap().tag.as_deref(),
                Some("node")
            );
            assert_eq!(
                path.supports_associated_relay_carrier(),
                slot.recipe.packet_path_facts().unwrap().associated,
            );
        }
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            listener.poll_accept(&mut context).is_pending(),
            "selection must remain cold"
        );
        proxy
            .protocols
            .shutdown_components(tokio::time::Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        let retired = std::sync::Arc::downgrade(&slot);
        drop(slot);
        drop(execution);
        drop(services);
        drop(proxy);
        assert!(
            retired.upgrade().is_none(),
            "a cached first candidate pinned the retired {name} owner"
        );
        drop(cached);
    }
}
