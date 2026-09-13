#![cfg(all(feature = "socks5", feature = "vless"))]

mod support;

use serde_json::{json, Value};
use support::interop::{socks5_udp_echo_sequence, spawn_udp_echo_count, TempMaterial};
use support::{free_port, free_udp_port, spawn_engine, wait_for, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_core::Network;
use zero_proxy::{Proxy, RunningProxy};

const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";
#[cfg(feature = "mieru")]
const MIERU_USERNAME: &str = "middle-relay";
#[cfg(feature = "mieru")]
const MIERU_PASSWORD: &str = "associated-secret";

#[derive(Clone, Copy)]
enum Carrier {
    Tls,
    H3,
    Mkcp,
    #[cfg(feature = "mieru")]
    Mieru,
}

#[tokio::test]
async fn vless_tls_middle_carries_mkcp_udp_without_direct_bypass() {
    run(&[Carrier::Tls], Carrier::Mkcp).await;
}

#[tokio::test]
async fn two_vless_middles_carry_h3_udp_through_recursive_shorter_prefixes() {
    run(&[Carrier::Tls, Carrier::H3], Carrier::H3).await;
}

#[tokio::test]
#[cfg(feature = "mieru")]
async fn vless_tls_middle_carries_mieru_udp_without_direct_bypass() {
    run(&[Carrier::Tls], Carrier::Mieru).await;
}

async fn run(middles: &[Carrier], final_carrier: Carrier) {
    support::interop::init_logs("zero_proxy=debug,zero_transport=debug,vless=debug");
    let material = TempMaterial::new("vless-associated-datagram-relay");
    let tls = material.tls();

    let first_port = free_port();
    let first = start_node(json!({
        "inbounds": [{
            "tag": "first",
            "listen": {"address": "127.0.0.1", "port": first_port},
            "protocol": {"type": "socks5"}
        }],
        "route": {"final": {"type": "direct"}}
    }));
    wait_for_listener(first_port).await;

    let mut outbounds = vec![json!({
        "tag": "first",
        "protocol": {
            "type": "socks5",
            "server": "127.0.0.1",
            "port": first_port
        }
    })];
    let mut proxies = vec!["first".to_owned()];
    let mut middle_nodes = Vec::new();
    let mut middle_ports = Vec::new();
    for (index, carrier) in middles.iter().copied().enumerate() {
        let port = carrier_port(carrier);
        let ready = free_port();
        let tag = format!("middle-{index}");
        let inbound = vless_inbound(carrier, &tls, port, ready, &tag);
        let node = start_node(inbound);
        wait_for_listener(ready).await;
        outbounds.push(json!({
            "tag": tag,
            "protocol": vless_outbound(carrier, &tls, port)
        }));
        proxies.push(tag);
        middle_ports.push(port);
        middle_nodes.push(node);
    }

    let final_port = carrier_port(final_carrier);
    let final_ready = free_port();
    let final_node = start_node(vless_inbound(
        final_carrier,
        &tls,
        final_port,
        final_ready,
        "final",
    ));
    wait_for_listener(final_ready).await;
    outbounds.push(json!({
        "tag": "final",
        "protocol": vless_outbound(final_carrier, &tls, final_port)
    }));
    proxies.push("final".to_owned());

    let socks = free_port();
    let client = start_node(json!({
        "inbounds": [{
            "tag": "local",
            "listen": {"address": "127.0.0.1", "port": socks},
            "protocol": {"type": "socks5"}
        }],
        "outbounds": outbounds,
        "outbound_groups": [{"tag": "chain", "type": "relay", "proxies": proxies}],
        "route": {"final": {"type": "route", "outbound": "chain"}}
    }));
    wait_for_listener(socks).await;

    let echo_port = free_udp_port();
    let echo = spawn_udp_echo_count(echo_port, 3).await;
    let packets: &[&[u8]] = &[b"first", &[0x5a; 1600], b"last"];
    let received = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        socks5_udp_echo_sequence(socks, echo_port, packets),
    )
    .await
    .expect("associated VLESS relay timed out");
    assert_eq!(
        received,
        packets
            .iter()
            .map(|packet| packet.to_vec())
            .collect::<Vec<_>>()
    );
    echo.await.unwrap();

    wait_for(
        "first hop to own the route to the first VLESS middle",
        || saw_target(&first, middle_ports[0], Network::Tcp),
    )
    .await;
    for (index, node) in middle_nodes.iter().enumerate() {
        let next_port = middle_ports.get(index + 1).copied().unwrap_or(final_port);
        let next_carrier = middles.get(index + 1).copied().unwrap_or(final_carrier);
        wait_for("each VLESS middle to own its next-hop route", || {
            saw_target(node, next_port, carrier_network(next_carrier))
        })
        .await;
        assert!(
            !saw_any_target(node, echo_port),
            "an intermediate VLESS hop bypassed its configured successor"
        );
    }
    wait_for("final VLESS node to own the destination route", || {
        saw_target(&final_node, echo_port, Network::Udp)
    })
    .await;
    assert!(!saw_any_target(&first, echo_port));

    let accounting = client.engine().clone();
    client.shutdown().await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(25), async {
        while accounting.stats_snapshot().udp_upstream.active_associations != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("associated VLESS packet paths retained UDP associations after shutdown");
    let stats = accounting.stats_snapshot().udp_upstream;
    assert_eq!(stats.created_associations, stats.closed_associations);

    first.shutdown().await.unwrap();
    for node in middle_nodes {
        node.shutdown().await.unwrap();
    }
    final_node.shutdown().await.unwrap();
}

fn start_node(config: Value) -> RunningProxy {
    spawn_engine(Proxy::new(RuntimeConfig::parse(&config.to_string()).unwrap()).unwrap())
}

fn carrier_port(carrier: Carrier) -> u16 {
    match carrier {
        Carrier::Tls => free_port(),
        Carrier::H3 | Carrier::Mkcp => free_udp_port(),
        #[cfg(feature = "mieru")]
        Carrier::Mieru => free_udp_port(),
    }
}

fn carrier_network(carrier: Carrier) -> Network {
    match carrier {
        Carrier::Tls => Network::Tcp,
        Carrier::H3 | Carrier::Mkcp => Network::Udp,
        #[cfg(feature = "mieru")]
        Carrier::Mieru => Network::Udp,
    }
}

fn vless_inbound(
    carrier: Carrier,
    tls: &support::interop::TestTlsMaterial,
    port: u16,
    ready: u16,
    tag: &str,
) -> Value {
    let mut protocol = json!({"type": "vless", "users": [{"id": ID}]});
    match carrier {
        Carrier::Tls => {
            protocol["tls"] = json!({
                "cert_path": tls.cert_path,
                "key_path": tls.key_path
            });
        }
        Carrier::H3 => {
            protocol["quic"] = json!({
                "cert_path": tls.cert_path,
                "key_path": tls.key_path
            });
            protocol["split_http"] = json!({"path": "/middle/", "mode": "auto"});
        }
        Carrier::Mkcp => protocol["mkcp"] = json!({"tti_ms": 20}),
        #[cfg(feature = "mieru")]
        Carrier::Mieru => {
            protocol = json!({
                "type": "mieru",
                "transport": "udp",
                "users": [{
                    "username": MIERU_USERNAME,
                    "password": MIERU_PASSWORD
                }]
            });
        }
    }
    json!({
        "inbounds": [
            {
                "tag": tag,
                "listen": {"address": "127.0.0.1", "port": port},
                "protocol": protocol
            },
            {
                "tag": format!("{tag}-ready"),
                "listen": {"address": "127.0.0.1", "port": ready},
                "protocol": {"type": "socks5"}
            }
        ],
        "route": {"final": {"type": "direct"}}
    })
}

fn vless_outbound(carrier: Carrier, tls: &support::interop::TestTlsMaterial, port: u16) -> Value {
    let mut protocol = json!({
        "type": "vless",
        "server": "127.0.0.1",
        "port": port,
        "id": ID
    });
    match carrier {
        Carrier::Tls => {
            protocol["tls"] = json!({
                "server_name": "localhost",
                "ca_cert_path": tls.cert_path
            });
        }
        Carrier::H3 => {
            protocol["quic"] = json!({
                "server_name": "localhost",
                "ca_cert_path": tls.cert_path
            });
            protocol["split_http"] = json!({
                "path": "/middle/",
                "mode": "stream-up",
                "xmux": {"max_connections": 1, "h_max_request_times": 3}
            });
        }
        Carrier::Mkcp => protocol["mkcp"] = json!({"tti_ms": 20}),
        #[cfg(feature = "mieru")]
        Carrier::Mieru => {
            protocol = json!({
                "type": "mieru",
                "transport": "udp",
                "server": "127.0.0.1",
                "port": port,
                "username": MIERU_USERNAME,
                "password": MIERU_PASSWORD
            });
        }
    }
    protocol
}

fn saw_target(node: &RunningProxy, port: u16, network: Network) -> bool {
    targets(node).any(|target| target == (port, network))
}

fn saw_any_target(node: &RunningProxy, port: u16) -> bool {
    targets(node).any(|target| target.0 == port)
}

fn targets(node: &RunningProxy) -> impl Iterator<Item = (u16, Network)> {
    node.active_sessions()
        .into_iter()
        .map(|session| (session.port, session.network))
        .chain(
            node.completed_sessions()
                .into_iter()
                .map(|session| (session.port, session.network)),
        )
}
