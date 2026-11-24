//! Selection uses the real published Source and its original provider.
use super::*;
use crate::{Proxy, ProxyHandle};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use std::{sync::Arc, time::Duration};
use zero_stack::packet::IPPROTO_UDP;

struct Fixture {
    proxy: Proxy,
    running: crate::RunningProxy,
}
impl Fixture {
    async fn start(inbound: bool) -> Self {
        let config = RuntimeConfig::parse(&serde_json::json!({
            "endpoints":[{"tag":"wg", "directions":{"inbound":inbound,"outbound":true},
                "listen":{"address":"127.0.0.1","port":0}, "protocol":{
                    "type":"wireguard", "private_key":STANDARD.encode([1;32]), "addresses":["10.0.0.1/32"],
                    "peers":[{"public_key":STANDARD.encode([2;32]), "endpoint":"127.0.0.1:51821", "allowed_ips":["10.0.0.0/24"]}]}}],
            "outbounds":[{"tag":"plain","protocol":{"type":"direct"}}],
            "outbound_groups":[
                {"tag":"choose","type":"selector","outbounds":["plain","wg"],"selected":"plain"},
                {"tag":"auto","type":"url_test","outbounds":["plain","wg"]},
                {"tag":"balance","type":"load_balance","outbounds":["plain","wg"],"strategy":"round_robin"}
            ],
            "route":{"final":{"type":"route","outbound":"wg"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let cold = proxy.protocols.prepare_packet_route_target_with_mode(
            &config,
            resolved(&proxy),
            Some(IPPROTO_ICMP),
            RouteMode::Translate,
        );
        assert!(
            matches!(cold, PacketRouteTarget::Unsupported),
            "declarations do not grant actual native I/O"
        );
        let running = proxy.spawn();
        ProxyHandle::new(
            zero_engine::EngineHandle::new(proxy.engine().clone()),
            proxy.clone(),
        )
        .apply_runtime_config_and_wait(config, Duration::from_secs(5))
        .await
        .unwrap();
        let snapshot = proxy.engine().runtime_snapshot();
        let plan = proxy
            .protocols
            .current_components(snapshot.config())
            .unwrap();
        let slot = plan.configured("wg").unwrap();
        assert!(plan.has_packet_provider(slot.role_id()));
        assert!(plan.has_device_carrier_provider(slot.role_id()));
        Self { proxy, running }
    }
    fn plan(&self) -> Arc<crate::component::catalog::ComponentPlan> {
        let snapshot = self.proxy.engine().runtime_snapshot();
        self.proxy
            .protocols
            .current_components(snapshot.resource_config())
            .unwrap()
    }
    fn target(&self, protocol: u8, mode: RouteMode) -> PacketRouteTarget {
        let snapshot = self.proxy.engine().runtime_snapshot();
        self.proxy.protocols.prepare_packet_route_target_admitted(
            snapshot.config(),
            resolved(&self.proxy),
            Some(protocol),
            mode,
            &zero_engine::EndpointAdmission::from_snapshot(&snapshot),
            Some(&self.plan()),
        )
    }
    fn assert_packet(&self, protocol: u8, mode: RouteMode, expected_translation: bool) {
        let PacketRouteTarget::Packet {
            tag,
            translated,
            role,
        } = self.target(protocol, mode)
        else {
            panic!("actual standard Packet provider was not selected");
        };
        assert_eq!(tag, "wg");
        assert_eq!(translated, expected_translation);
        assert_eq!(role, self.plan().configured("wg").unwrap().role_id());
    }
}
fn resolved(proxy: &Proxy) -> ResolvedOutbound<'_> {
    proxy
        .engine()
        .resolve_route_decision(zero_engine::RouteDecision::Route("wg".into()))
        .unwrap()
        .0
}
#[tokio::test]
async fn outbound_only_source_preserves_correlated_flows_and_opt_in_translation() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let live = Fixture::start(false).await;
        for protocol in [IPPROTO_TCP, IPPROTO_UDP] {
            for mode in [RouteMode::Auto, RouteMode::Flow, RouteMode::Translate] {
                assert!(matches!(
                    live.target(protocol, mode),
                    PacketRouteTarget::Flow
                ));
            }
            assert!(matches!(
                live.target(protocol, RouteMode::Packet),
                PacketRouteTarget::Unsupported
            ));
        }
        assert!(matches!(
            live.target(IPPROTO_ICMP, RouteMode::Auto),
            PacketRouteTarget::Unsupported
        ));
        live.assert_packet(IPPROTO_ICMP, RouteMode::Translate, true);
        live.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn bidirectional_source_preserves_native_packet_selection_and_exact_role_generation() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let live = Fixture::start(true).await;
        for protocol in [IPPROTO_TCP, IPPROTO_UDP, IPPROTO_ICMP] {
            live.assert_packet(protocol, RouteMode::Auto, false);
            live.assert_packet(protocol, RouteMode::Packet, false);
        }
        live.assert_packet(IPPROTO_ICMP, RouteMode::Translate, true);
        for protocol in [IPPROTO_TCP, IPPROTO_UDP] {
            assert!(matches!(
                live.target(protocol, RouteMode::Translate),
                PacketRouteTarget::Flow
            ));
        }
        let source = live
            .proxy
            .system_tun_source_registry()
            .owner("endpoint/wg")
            .expect("same real endpoint Source");
        live.running.shutdown().await.unwrap();
        let terminal = source.terminal_for_test().unwrap();
        assert!(
            terminal.failure().is_none()
                && terminal.task_failures().is_empty()
                && terminal.native_bound_failures().is_empty()
        );
        assert!(terminal.closed_io_receipts() > 0 && terminal.joined_tasks() > 0);
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn strict_packet_flows_select_installed_device_stream_and_datagram_without_private_converters(
) {
    tokio::time::timeout(Duration::from_secs(20), async {
        let live = Fixture::start(false).await;
        let mut session = zero_core::Session::new(
            0,
            zero_core::Address::Ipv4([10, 0, 0, 2]),
            443,
            zero_core::Network::Tcp,
            zero_core::ProtocolType::new("test"),
        );
        live.proxy
            .engine()
            .prepare_session(&mut session, "incoming")
            .unwrap();
        let services = live
            .proxy
            .tcp_runtime_services()
            .for_inbound("incoming".into())
            .with_business_identity(&session)
            .unwrap();
        let execution = services.execution();
        let plan = execution.components.as_ref().unwrap();
        let expected = plan.configured("wg").unwrap();
        let resolved = resolved(&live.proxy);
        for mode in [
            RouteMode::Auto,
            RouteMode::Packet,
            RouteMode::Flow,
            RouteMode::Translate,
        ] {
            let selected = services
                .prepare_tcp_outbound(&resolved, mode)
                .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
            let crate::inventory::PreparedTcpOutbound::Single(
                crate::inventory::PreparedTcpCandidate {
                    execution: crate::inventory::PreparedTcpCandidateExecution::Native(actual),
                    ..
                },
            ) = selected
            else {
                panic!("strict Packet restored a private TCP converter");
            };
            assert_eq!(actual.instance(), expected.instance_id());
            let context = crate::protocol_registry::UdpAdapterContext::new(
                services.config(),
                crate::protocol_registry::UdpRuntimeServices::new(services.clone()),
            );
            let selected = live
                .proxy
                .protocols
                .prepare_udp_outbound(context, &session, &resolved, b"strict", mode)
                .unwrap_or_else(|failure| panic!("{}: {}", failure.stage, failure.error));
            let crate::inventory::PreparedUdpOutbound::Single(
                crate::inventory::PreparedUdpLeafCandidate::DatagramFlow(actual),
            ) = selected
            else {
                panic!("strict Packet restored a private UDP converter");
            };
            assert_eq!(
                actual.current_slot(&execution).unwrap().instance_id(),
                expected.instance_id()
            );
        }
        drop(execution);
        drop(services);
        live.running.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[path = "endpoint/policy.rs"]
mod policy;
