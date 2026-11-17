//! Cold selection and negative admission only. These tests manufacture no
//! accepted/Source caller and make no original-close or wire-interop claim.
#![cfg(all(feature = "shadowsocks", feature = "socks5", feature = "vless"))]
use super::*;
use crate::{inventory::PreparedUdpOutbound, protocol_registry::UdpRuntimeServices, Proxy};
use zero_core::{Address, Network, ProtocolType};

fn proxy(members: &[&str]) -> Proxy {
    let config = zero_config::RuntimeConfig::parse(&serde_json::json!({
        "outbounds":[
            {"tag":"a","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":21001,"cipher":"chacha20-ietf-poly1305","password":"a-password"}},
            {"tag":"b","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":21002,"cipher":"chacha20-ietf-poly1305","password":"b-password"}},
            {"tag":"c","protocol":{"type":"shadowsocks","server":"127.0.0.1","port":21003,"cipher":"chacha20-ietf-poly1305","password":"c-password"}},
            {"tag":"socks","protocol":{"type":"socks5","server":"127.0.0.1","port":21004}},
            {"tag":"vless","protocol":{"type":"vless","server":"127.0.0.1","port":21005,"id":"01234567-89ab-cdef-0123-456789abcdef"}}
        ],
        "outbound_groups":[{"tag":"chain","type":"relay","proxies":members}],
        "route":{"final":{"type":"route","outbound":"chain"}}
    }).to_string()).unwrap();
    let mut proxy = Proxy::new(config).unwrap();
    // The real factories/adapter state stay installed. Their executable claim
    // projections are absent, so a successful selection cannot mask old calls.
    proxy.protocols.registry = Arc::new(proxy.protocols.registry.as_ref().clone());
    proxy
}
fn session(proxy: &Proxy) -> Session {
    let mut session = Session::new(
        0,
        Address::Ipv4([203, 0, 113, 9]),
        53,
        Network::Udp,
        ProtocolType::new("cold-selection"),
    );
    proxy
        .engine()
        .prepare_session(&mut session, "incoming")
        .unwrap();
    session
}

#[tokio::test]
async fn whole_packet_codec_relay_selects_real_roles_without_any_executable_claim() {
    for (members, root, donors) in [
        (vec!["a", "b"], "a", 0),
        (vec!["a", "b", "c"], "a", 0),
        (vec!["socks", "a", "b"], "socks", 0),
        (vec!["socks", "vless", "a", "b"], "vless", 1),
    ] {
        let proxy = proxy(&members);
        let session = session(&proxy);
        let services = proxy
            .tcp_runtime_services()
            .for_inbound("incoming".into())
            .with_business_identity(&session)
            .unwrap();
        let (resolved, _) = proxy
            .engine()
            .resolve_route_decision(zero_engine::RouteDecision::Route("chain".into()))
            .unwrap();
        let prepared = proxy
            .protocols
            .prepare_udp_outbound(
                UdpAdapterContext::new(
                    services.config(),
                    UdpRuntimeServices::new(services.clone()),
                ),
                &session,
                &resolved,
                b"query",
                RouteMode::Flow,
            )
            .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
        let PreparedUdpOutbound::Relay(prepared) = prepared else {
            panic!("standard codec relay was not selected")
        };
        let PreparedUdpRelayChain { request, .. } = *prepared;
        let PacketPathCarrierRequest(recipe) = request.carrier;
        assert_eq!(recipe.descriptor().tag.as_deref(), Some(root));
        assert_eq!(recipe.binding().prefix.len(), donors);
        assert_eq!(request.datagram.descriptor().tag, *members.last().unwrap());
        let budgets = proxy.protocols.runtime_host().original_budgets();
        assert_eq!(
            budgets.carrier.snapshot().charged,
            0,
            "selection opens no physical original"
        );
    }
}

#[tokio::test]
async fn a_recipe_from_another_network_epoch_is_rejected_before_any_original_opens() {
    let proxy = proxy(&["a", "b"]);
    let session = session(&proxy);
    let services = proxy
        .tcp_runtime_services()
        .for_inbound("incoming".into())
        .with_business_identity(&session)
        .unwrap();
    let execution = services.execution();
    let plan = execution.components.as_ref().unwrap();
    let slot = plan.configured("a").unwrap();
    let projection = proxy
        .protocols
        .registry
        .prepare_cold_packet_path_projection(&services.config().outbounds[0])
        .unwrap()
        .unwrap();
    let current = NetworkGeneration(execution.upstream().network_generation());
    let stale = RolePacketPath::prepare(
        &slot,
        &projection,
        None,
        plan,
        NetworkGeneration(current.0 + 1),
    )
    .unwrap();
    let matching = RolePacketPath::prepare(&slot, &projection, None, plan, current).unwrap();
    assert_ne!(
        stale.descriptor().cache_key,
        matching.descriptor().cache_key
    );
    let error = match stale
        .build(
            crate::protocol_registry::PacketPathExecutionServices::from_tcp_execution(&execution),
        )
        .await
    {
        Ok(_) => panic!("stale recipe acquired a native original"),
        Err(error) => error,
    };
    assert_eq!(
        crate::transport::channel_failure(&error).unwrap().reason,
        zero_platform_tokio::contract::Reason::StalePlan
    );
    // A Business identity alone also does not grant an original caller Scope.
    let error = match matching
        .build(
            crate::protocol_registry::PacketPathExecutionServices::from_tcp_execution(&execution),
        )
        .await
    {
        Ok(_) => panic!("cold business facts fabricated a real caller"),
        Err(error) => error,
    };
    assert_eq!(
        crate::transport::channel_failure(&error).unwrap().reason,
        zero_platform_tokio::contract::Reason::Unsupported
    );
    assert_eq!(
        proxy
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .snapshot()
            .charged,
        0
    );
}

#[tokio::test]
async fn cold_standard_prefixes_never_construct_private_operations_or_codec_executors() {
    for members in [vec!["socks", "a"], vec!["socks", "a", "b", "c"]] {
        let proxy = library_proxy(&members, None);
        let session = session(&proxy);
        let services = proxy.tcp_runtime_services();
        assert!(services.execution().invocation.is_none());
        let (resolved, _) = proxy
            .engine()
            .resolve_route_decision(zero_engine::RouteDecision::Route("chain".into()))
            .unwrap();
        let selected = proxy
            .protocols
            .prepare_udp_outbound(
                UdpAdapterContext::new(
                    services.config(),
                    UdpRuntimeServices::new(services.clone()),
                ),
                &session,
                &resolved,
                b"query",
                RouteMode::Flow,
            )
            .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
        let PreparedUdpOutbound::Relay(selected) = selected else {
            panic!("expected existing packet codec grammar")
        };
        let PreparedUdpRelayChain { request, .. } = *selected;
        let PacketPathCarrierRequest(recipe) = &request.carrier;
        assert!(recipe.binding().prefix.is_empty());
        assert_eq!(request.datagram.descriptor().tag, *members.last().unwrap());
        let descriptor = request.carrier.descriptor();
        assert_eq!(descriptor.tag.as_deref(), Some("socks"));
        assert_eq!(
            proxy
                .protocols
                .runtime_host()
                .original_budgets()
                .carrier
                .snapshot()
                .charged,
            0
        );
    }
}

fn library_proxy(members: &[&str], disabled: Option<&str>) -> Proxy {
    let fixture = proxy(members);
    let mut config = fixture
        .engine()
        .runtime_snapshot()
        .config()
        .as_ref()
        .clone();
    if let Some(tag) = disabled {
        config
            .outbounds
            .iter_mut()
            .find(|input| input.tag() == tag)
            .unwrap()
            .udp
            .enabled = false;
    }
    let mut proxy = Proxy::new(config).unwrap();
    proxy.protocols.registry = Arc::new(proxy.protocols.registry.as_ref().clone());
    proxy
}

#[tokio::test]
async fn cold_only_codec_suffixes_still_enforce_each_members_udp_policy() {
    for disabled in ["a", "b"] {
        let proxy = library_proxy(&["socks", "a", "b"], Some(disabled));
        let session = session(&proxy);
        let services = proxy.tcp_runtime_services();
        let (resolved, _) = proxy
            .engine()
            .resolve_route_decision(zero_engine::RouteDecision::Route("chain".into()))
            .unwrap();
        let failure = proxy
            .protocols
            .prepare_udp_outbound(
                UdpAdapterContext::new(
                    services.config(),
                    UdpRuntimeServices::new(services.clone()),
                ),
                &session,
                &resolved,
                b"query",
                RouteMode::Flow,
            )
            .err()
            .expect("a cold codec does not bypass UDP policy");
        assert_eq!(failure.stage, "udp_policy");
        assert_eq!(
            proxy
                .protocols
                .runtime_host()
                .original_budgets()
                .carrier
                .snapshot()
                .charged,
            0
        );
    }
}

#[tokio::test]
async fn compatibility_codec_selection_never_invents_a_nonroot_socks_association() {
    let proxy = library_proxy(&["a", "socks", "b"], None);
    let session = session(&proxy);
    let services = proxy.tcp_runtime_services();
    let (resolved, _) = proxy
        .engine()
        .resolve_route_decision(zero_engine::RouteDecision::Route("chain".into()))
        .unwrap();
    let failure = proxy
        .protocols
        .prepare_udp_outbound(
            UdpAdapterContext::new(services.config(), UdpRuntimeServices::new(services.clone())),
            &session,
            &resolved,
            b"query",
            RouteMode::Flow,
        )
        .err()
        .expect("SOCKS remains root-only even with a pure codec tail");
    assert_eq!(failure.stage, "no_udp_relay_final_hop");
    assert_eq!(
        proxy
            .protocols
            .runtime_host()
            .original_budgets()
            .carrier
            .snapshot()
            .charged,
        0
    );
}
