#![cfg(feature = "wireguard")]

use crate::support;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use gotatun::x25519::{PublicKey, StaticSecret};
use std::net::IpAddr;
use tokio::{
    net::UdpSocket,
    time::{timeout, Duration},
};
use wireguard::{
    runtime::{PeerTunnel, TunnelAction},
    validation::{validate_outbound, OutboundInput, PeerInput},
};
use zero_api::*;
use zero_config::{EndpointProtocolConfig, RuntimeConfig};
use zero_engine::EngineHandle;
use zero_proxy::{Proxy, ProxyHandle, RunningProxy};
use zero_stack::packet;

fn public(seed: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([seed; 32])).as_bytes())
}
struct LocalEndpoint {
    config: RuntimeConfig,
    proxy: Proxy,
    control: ProxyHandle,
    running: RunningProxy,
    socket: UdpSocket,
    peer: PeerTunnel,
}
impl LocalEndpoint {
    async fn start(outbound: bool) -> Self {
        let port = support::free_udp_port();
        let config = RuntimeConfig::parse(&serde_json::json!({
            "endpoints":[{"tag":"local", "directions":{"inbound":true,"outbound":outbound},
                "listen":{"address":"127.0.0.1","port":port},
                "protocol":{"type":"wireguard","private_key":STANDARD.encode([181;32]),
                    "addresses":["10.70.1.11/24","fd00:70::11/64"],
                    "peers":[{"public_key":public(182),"allowed_ips":["10.70.1.2/32","fd00:70::2/128"]}]}}],
            "route":{"rules":[],"final":{"type":"reject"}}
        }).to_string()).unwrap();
        let proxy = Proxy::new(config.clone()).unwrap();
        let control = ProxyHandle::new(EngineHandle::new(proxy.engine().clone()), proxy.clone());
        let running = support::spawn_engine(proxy.clone());
        support::wait_for("local endpoint running", || {
            endpoint(&control).state == EndpointRuntimeState::Running
                && endpoint(&control).counters.active_packet_routes.is_some()
        })
        .await;
        let private = STANDARD.encode([182; 32]);
        let public = public(181);
        let peers = [PeerInput {
            public_key: &public,
            pre_shared_key: None,
            endpoint: "127.0.0.1:9",
            allowed_ips: &["0.0.0.0/0", "::/0"],
            keepalive_secs: 0,
            reserved: &[],
        }];
        let profile = validate_outbound(OutboundInput {
            private_key: &private,
            addresses: &["10.70.1.2/32", "fd00:70::2/128"],
            mtu: 1420,
            peers: &peers,
        })
        .unwrap();
        let peer = PeerTunnel::from_validated(&profile, 0).unwrap();
        let socket = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
        socket.connect(("127.0.0.1", port)).await.unwrap();
        Self {
            config,
            proxy,
            control,
            running,
            socket,
            peer,
        }
    }
    async fn exchange(&mut self, request: &[u8]) -> Vec<u8> {
        for action in self.peer.send_ip_packet(request).unwrap() {
            if let TunnelAction::SendNetwork(bytes) = action {
                self.socket.send(&bytes).await.unwrap();
            }
        }
        let mut buffer = [0; 65535];
        loop {
            let size = self.socket.recv(&mut buffer).await.unwrap();
            for action in self
                .peer
                .receive_datagram(Some(self.socket.peer_addr().unwrap()), &buffer[..size])
                .unwrap()
            {
                match action {
                    TunnelAction::SendNetwork(bytes) => {
                        self.socket.send(&bytes).await.unwrap();
                    }
                    TunnelAction::ReceiveIp { packet, .. } => return packet,
                }
            }
        }
    }
    async fn roundtrip(&mut self, request: &[u8]) -> Vec<u8> {
        timeout(Duration::from_secs(5), self.exchange(request))
            .await
            .expect("local Echo timed out")
    }
}
fn endpoint(control: &ProxyHandle) -> EndpointSnapshot {
    let QueryResponse::Endpoint(e) = control
        .query(QueryRequest::Endpoint(EndpointGetQuery {
            endpoint_id: "endpoint:local".into(),
        }))
        .unwrap()
    else {
        panic!("wrong response")
    };
    e
}
fn echo(source: &str, destination: &str) -> Vec<u8> {
    let source: IpAddr = source.parse().unwrap();
    let destination = destination.parse().unwrap();
    let mut message = b"\x08\0\0\0\0\0\0\x07endpoint-local-ping".to_vec();
    if source.is_ipv6() {
        message[0] = 128;
    }
    packet::build_icmp_echo_tunnel_probe(
        &packet::IcmpEchoRequest {
            source,
            destination,
            message: &message,
        },
        0x1234,
        source,
        1420,
    )
    .unwrap()
}
fn assert_reply(request: &[u8], response: &[u8]) {
    let request = packet::parse_icmp_echo_request(request).unwrap();
    let response =
        packet::parse_icmp_echo_reply(response).expect("expected Echo Reply, not a policy error");
    assert_eq!(response.source, request.destination);
    assert_eq!(response.destination, request.source);
    assert_eq!(&response.message[4..], &request.message[4..]);
}

#[tokio::test]
async fn authenticated_ipv4_and_ipv6_echo_is_local_without_a_flow_or_forwarding_route() {
    let mut zero = LocalEndpoint::start(false).await;
    let idle = endpoint(&zero.control);
    assert_eq!(idle.counters.active_packet_routes, Some(0));
    for (src, dst) in [("10.70.1.2", "10.70.1.11"), ("fd00:70::2", "fd00:70::11")] {
        let request = echo(src, dst);
        let response = zero.roundtrip(&request).await;
        assert_reply(&request, &response);
    }
    let live = endpoint(&zero.control);
    assert_eq!(live.counters.active_stream_flows, Some(0));
    assert_eq!(live.counters.active_datagram_flows, Some(0));
    assert_eq!(live.counters.active_packet_routes, Some(0));
    assert_eq!(live.counters.inner_rx_packets, Some(2));
    assert_eq!(live.counters.inner_tx_packets, Some(2));
    assert_eq!(zero.proxy.stats_snapshot().bytes_up, 0);
    assert_eq!(zero.proxy.stats_snapshot().bytes_down, 0);
    assert_eq!(
        zero.proxy
            .engine()
            .packet_routes_snapshot(&PacketRouteListQuery::default())
            .total,
        0
    );
    zero.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_echo_does_not_claim_other_ips_in_assigned_prefix_and_follows_address_reload() {
    let mut zero = LocalEndpoint::start(true).await;
    let old = echo("10.70.1.2", "10.70.1.11");
    let response = zero.roundtrip(&old).await;
    assert_reply(&old, &response);
    for (src, dst) in [("10.70.1.2", "10.70.1.12"), ("fd00:70::2", "fd00:70::12")] {
        let response = zero.roundtrip(&echo(src, dst)).await;
        assert!(packet::parse_icmp_echo_reply(&response).is_none());
        // parse_icmp_error is the TCP/UDP error demultiplexer; this quote is Echo.
        assert_eq!(packet::ip_source(&response), Some(dst.parse().unwrap()));
        assert_eq!(
            packet::ip_destination(&response),
            Some(src.parse().unwrap())
        );
        let offset = if response[0] >> 4 == 4 { 20 } else { 40 };
        assert_eq!(response[offset], if offset == 20 { 3 } else { 1 });
    }
    let EndpointProtocolConfig::Wireguard { addresses, .. } =
        &mut zero.config.endpoints[0].protocol;
    addresses[0] = "10.70.1.13/24".into();
    // Reparse canonical configuration to regenerate its internal role views.
    let next = RuntimeConfig::parse(&serde_json::to_string(&zero.config).unwrap()).unwrap();
    zero.control
        .apply_runtime_config_and_wait(next, Duration::from_secs(5))
        .await
        .unwrap();
    let request = echo("10.70.1.2", "10.70.1.13");
    let response = zero.roundtrip(&request).await;
    assert_reply(&request, &response);
    let response = zero.roundtrip(&old).await;
    assert!(packet::parse_icmp_echo_reply(&response).is_none());
    zero.running.shutdown().await.unwrap();
}

#[tokio::test]
async fn local_echo_requires_allowed_peer_source_and_inbound_direction() {
    let mut zero = LocalEndpoint::start(true).await;
    let valid = echo("10.70.1.2", "10.70.1.11");
    let response = zero.roundtrip(&valid).await;
    assert_reply(&valid, &response);
    let forged = echo("10.70.1.99", "10.70.1.11");
    assert!(timeout(Duration::from_millis(400), zero.exchange(&forged))
        .await
        .is_err());
    let before = endpoint(&zero.control);
    zero.control
        .execute_acknowledged(CommandRequest::EndpointSetDirections(
            EndpointSetDirectionsCommand {
                endpoint_id: before.endpoint_id,
                directions: EndpointDirections::outbound_only(),
                persistence: EndpointPersistence::RuntimeOnly,
                expected_core_instance_id: Some(before.core_instance_id),
                expected_intent_revision: Some(before.intent_revision),
            },
        ))
        .await
        .unwrap();
    assert!(timeout(Duration::from_millis(400), zero.exchange(&valid))
        .await
        .is_err());
    let denied = endpoint(&zero.control);
    assert!(!denied.effective.inbound);
    assert!(denied.effective.outbound);
    assert_eq!(denied.counters.inner_tx_packets, Some(1));
    assert!(
        denied.counters.dropped_packets.unwrap() >= 2,
        "before={:?}, denied={:?}",
        before.counters,
        denied.counters
    );
    zero.running.shutdown().await.unwrap();
}
