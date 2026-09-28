use std::net::IpAddr;

use zero_core::{Address, Network, ProtocolType, Session};
use zero_engine::RouteDecision;
#[cfg(feature = "wireguard")]
use zero_stack::packet::{IPPROTO_ICMP, IPPROTO_ICMPV6};
use zero_stack::packet::{IPPROTO_TCP, IPPROTO_UDP};

use super::{InboundRouteRuntimeFactory, RuntimeConfig, SharedIngressRuntimeServices};
use crate::{inventory::PacketRouteTarget, runtime::Proxy};

fn proxy() -> Proxy {
    Proxy::new(
        RuntimeConfig::parse(
            &serde_json::json!({
                "runtime":{"dns":{
                    "servers":{"upstream":{"type":"system"}},
                    "default_server":"upstream",
                    "answer":{"type":"fake_ip","cidr":"198.18.0.0/15",
                        "ipv6_cidr":"fdfe::/64"}
                }},
                "route":{"rules":[{
                    "condition":{"type":"domain","values":["allowed.example"]},
                    "action":{"type":"direct"},"mode":"flow"
                }],"final":{"type":"reject"},"final_mode":"packet"}
            })
            .to_string(),
        )
        .unwrap(),
    )
    .unwrap()
}

fn factory(proxy: &Proxy) -> InboundRouteRuntimeFactory {
    InboundRouteRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        "tun-dns-test".to_owned(),
    )
}

fn address(ip: IpAddr) -> Address {
    match ip {
        IpAddr::V4(ip) => Address::Ipv4(ip.octets()),
        IpAddr::V6(ip) => Address::Ipv6(ip.octets()),
    }
}

async fn allocate(proxy: &Proxy, query_type: u16) -> IpAddr {
    let mut query = vec![0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0];
    for label in ["allowed", "example"] {
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&query_type.to_be_bytes());
    query.extend_from_slice(&1_u16.to_be_bytes());
    let response = proxy.resolver.answer_udp_query(&query).await.unwrap();
    assert_eq!(response[3] & 15, 0);
    assert_eq!(&response[6..8], &1_u16.to_be_bytes());
    if query_type == 1 {
        IpAddr::V4(
            <[u8; 4]>::try_from(&response[response.len() - 4..])
                .unwrap()
                .into(),
        )
    } else {
        IpAddr::V6(
            <[u8; 16]>::try_from(&response[response.len() - 16..])
                .unwrap()
                .into(),
        )
    }
}

#[tokio::test]
async fn fake_ip_packets_restore_domains_before_a_forced_packet_default_reject_is_evaluated() {
    let proxy = proxy();
    let factory = factory(&proxy);
    let runtime = factory.for_connection(None);
    for query_type in [1, 28] {
        let destination = allocate(&proxy, query_type).await;
        for (protocol, network) in [(IPPROTO_TCP, Network::Tcp), (IPPROTO_UDP, Network::Udp)] {
            assert!(matches!(
                factory.packet_route_target(destination, Some(protocol)),
                PacketRouteTarget::Flow
            ));
            let mut session =
                Session::new(1, address(destination), 443, network, ProtocolType::UNKNOWN);
            session.transparent_target = true;
            runtime
                .tcp_runtime
                .resolve_fake_ip_target(&mut session)
                .await
                .unwrap();
            assert_eq!(
                session.target,
                Address::Domain("allowed.example".to_owned())
            );
            assert_eq!(
                runtime.tcp_runtime.route_trace(&session).await.decision,
                RouteDecision::Direct
            );
        }
    }
    assert!(matches!(
        factory.packet_route_target("203.0.113.1".parse().unwrap(), Some(IPPROTO_TCP)),
        PacketRouteTarget::Block
    ));
}

#[tokio::test]
async fn unmapped_fake_ip_packets_use_the_existing_fail_closed_flow_path() {
    let proxy = proxy();
    let factory = factory(&proxy);
    let runtime = factory.for_connection(None);
    for destination in ["198.18.0.42", "fdfe::42"] {
        let destination = destination.parse().unwrap();
        assert!(matches!(
            factory.packet_route_target(destination, Some(IPPROTO_UDP)),
            PacketRouteTarget::Flow
        ));
        let mut session = Session::new(
            1,
            address(destination),
            443,
            Network::Udp,
            ProtocolType::UNKNOWN,
        );
        let error = runtime
            .tcp_runtime
            .resolve_fake_ip_target(&mut session)
            .await
            .unwrap_err();
        assert_eq!(error.code(), "fake_ip_reverse_missing");
    }
}

#[cfg(feature = "wireguard")]
#[tokio::test]
async fn fake_ip_packets_never_escape_through_a_native_wireguard_packet_sink() {
    use base64::{engine::general_purpose::STANDARD, Engine as _};
    let mut value = serde_json::to_value(proxy().engine().config().as_ref()).unwrap();
    value["outbounds"] = serde_json::json!([{"tag":"wg","protocol":{
        "type":"wireguard","private_key":STANDARD.encode([1;32]),
        "addresses":["10.222.0.2/32"],"peers":[{
            "public_key":STANDARD.encode([2;32]),"endpoint":"127.0.0.1:51820",
            "allowed_ips":["0.0.0.0/0","::/0"]
        }]
    }}]);
    value["route"]["final"] = serde_json::json!({"type":"route","outbound":"wg"});
    let proxy = Proxy::new(RuntimeConfig::parse(&value.to_string()).unwrap()).unwrap();
    let factory = factory(&proxy);
    for query_type in [1, 28] {
        let destination = allocate(&proxy, query_type).await;
        for protocol in [IPPROTO_TCP, IPPROTO_UDP] {
            assert!(matches!(
                factory.packet_route_target(destination, Some(protocol)),
                PacketRouteTarget::Flow
            ));
        }
        assert!(matches!(
            factory.packet_route_target(
                destination,
                Some(if destination.is_ipv4() {
                    IPPROTO_ICMP
                } else {
                    IPPROTO_ICMPV6
                })
            ),
            PacketRouteTarget::Unsupported
        ));
    }
    assert!(matches!(
        factory.packet_route_target("203.0.113.1".parse().unwrap(), Some(IPPROTO_TCP)),
        PacketRouteTarget::Packet {
            translated: false,
            ..
        }
    ));
}
