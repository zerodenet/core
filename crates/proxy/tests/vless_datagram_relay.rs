#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;
const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";

#[tokio::test]
async fn native_datagram_carriers_keep_tcp_and_udp_inside_the_relay_prefix() {
    for carrier in ["mkcp", "quic", "h3", "hysteria"] {
        run(carrier, None, 0).await;
    }
}
#[tokio::test]
async fn datagram_relay_reaches_pinned_official_carrier_servers() {
    let Some(binary) = support::interop::require_env("XRAY_BIN") else {
        return;
    };
    for carrier in ["mkcp", "h3", "hysteria"] {
        run(carrier, Some(&binary), 0).await;
    }
}
#[tokio::test]
#[cfg(feature = "shadowsocks")]
async fn datagram_carriers_cross_multiple_independently_accounted_packet_hops() {
    for layers in [1, 2] {
        for carrier in ["mkcp", "quic", "h3", "hysteria"] {
            run(carrier, None, layers).await;
        }
    }
    if let Some(binary) = support::interop::require_env("XRAY_BIN") {
        for carrier in ["mkcp", "h3", "hysteria"] {
            run(carrier, Some(&binary), 2).await;
        }
    }
}
#[tokio::test]
async fn stream_packet_prefix_carries_datagram_transports_and_reclaims_flows() {
    for carrier in ["mkcp", "h3"] {
        run_with_prefix(carrier, None, 0, true).await;
    }
    if let Some(binary) = support::interop::require_env("XRAY_BIN") {
        run_with_prefix("h3", Some(&binary), 0, true).await;
    }
}
async fn run(carrier: &str, binary: Option<&str>, layers: usize) {
    run_with_prefix(carrier, binary, layers, false).await;
}
async fn run_with_prefix(carrier: &str, binary: Option<&str>, layers: usize, stream_prefix: bool) {
    support::interop::init_logs("zero_transport=debug,vless=debug");
    eprintln!(
        "datagram relay carrier={carrier} official={}",
        binary.is_some()
    );
    let first_port = free_port();
    let tunnel = free_udp_port();
    let ready = free_port();
    let socks = free_port();
    let material = TempMaterial::new("vless-datagram-relay");
    let tls = material.tls();
    let prefix_inbound = if stream_prefix {
        json!({"type":"vless","users":[{"id":ID}],"tls":{"cert_path":tls.cert_path,"key_path":tls.key_path}})
    } else {
        json!({"type":"socks5"})
    };
    let prefix_outbound = if stream_prefix {
        json!({"type":"vless","server":"127.0.0.1","port":first_port,"id":ID,"tls":{"server_name":"localhost","ca_cert_path":tls.cert_path}})
    } else {
        json!({"type":"socks5","server":"127.0.0.1","port":first_port})
    };
    let first = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
        "inbounds":[{"tag":"prefix","listen":{"address":"127.0.0.1","port":first_port},"protocol":prefix_inbound}],
        "route":{"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap());
    wait_for_listener(first_port).await;
    let mut prefixes = Vec::new();
    let mut proxies = vec!["first".to_owned()];
    let mut outbounds = vec![json!({"tag":"first","protocol":prefix_outbound})];
    for index in 0..layers {
        let port = free_port();
        let tag = format!("packet-{index}");
        let protocol = json!({"type":"shadowsocks","cipher":"2022-blake3-aes-128-gcm","password":"AAAAAAAAAAAAAAAAAAAAAA=="});
        let server = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({"inbounds":[{"tag":tag,"listen":{"address":"127.0.0.1","port":port},"protocol":protocol}],"route":{"final":{"type":"direct"}}}).to_string()).unwrap()).unwrap());
        wait_for_listener(port).await;
        let mut outbound = protocol;
        outbound["server"] = json!("127.0.0.1");
        outbound["port"] = json!(port);
        outbounds.push(json!({"tag":tag,"protocol":outbound}));
        proxies.push(tag);
        prefixes.push((port, server));
    }
    let mut inbound = json!({"type":"vless","users":[{"id":ID}]});
    let mut outbound = json!({"type":"vless","server":"127.0.0.1","port":tunnel,"id":ID});
    let mut reference = json!({"security":"tls","tlsSettings":{"alpn":["h3"],"certificates":[{"certificateFile":tls.cert_path,"keyFile":tls.key_path}]}});
    if carrier == "mkcp" {
        inbound["mkcp"] = json!({});
        outbound["mkcp"] = json!({});
        reference = json!({"network":"mkcp"});
    } else {
        inbound["quic"] = json!({"cert_path":tls.cert_path,"key_path":tls.key_path});
        outbound["quic"] = json!({"server_name":"localhost","ca_cert_path":tls.cert_path});
    }
    if carrier == "h3" {
        inbound["split_http"] = json!({"path":"/relay/","mode":"auto"});
        outbound["split_http"] = json!({"path":"/relay/","mode":"stream-up","xmux":{"max_connections":1,"h_max_request_times":3}});
        reference["network"] = json!("xhttp");
        reference["xhttpSettings"] = json!({"path":"/relay/","mode":"auto"});
    }
    if carrier == "hysteria" {
        inbound["hysteria"] = json!({"auth":"relay-secret"});
        outbound["hysteria"] = inbound["hysteria"].clone();
        reference["network"] = json!("hysteria");
        reference["hysteriaSettings"] = json!({"version":2,"auth":"relay-secret"});
    }
    let mut process = None;
    let mut server = None;
    if let Some(binary) = binary {
        let path = material.path("server.json");
        std::fs::write(&path, json!({"log":{"loglevel":"warning"},"inbounds":[
            {"listen":"127.0.0.1","port":tunnel,"protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID}]},"streamSettings":reference},
            {"listen":"127.0.0.1","port":ready,"protocol":"socks"}],"outbounds":[{"protocol":"freedom"}]
        }).to_string()).unwrap();
        process = Some(ExternalProcess::start(
            binary.to_owned(),
            &["run", "-c", path.to_str().unwrap()],
            &material,
            "server",
        ));
    } else {
        server = Some(spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
            "inbounds":[{"tag":"final","listen":{"address":"127.0.0.1","port":tunnel},"protocol":inbound},
            {"tag":"ready","listen":{"address":"127.0.0.1","port":ready},"protocol":{"type":"socks5"}}],"route":{"final":{"type":"direct"}}
        }).to_string()).unwrap()).unwrap()));
    }
    wait_for_listener(ready).await;
    outbounds.push(json!({"tag":"final","protocol":outbound}));
    proxies.push("final".to_owned());
    let client = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
        "inbounds":[{"tag":"local","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "outbounds":outbounds,
        "outbound_groups":[{"tag":"chain","type":"relay","proxies":proxies}],
        "route":{"final":{"type":"route","outbound":"chain"}}
    }).to_string()).unwrap()).unwrap());
    wait_for_listener(socks).await;
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        for byte in 0..2 {
            let port = free_port();
            let payload = vec![byte; 65537];
            let echo = spawn_tcp_echo(port, payload.len()).await;
            assert_eq!(
                socks5_tcp_echo_once(socks, port, &payload).await.unwrap(),
                payload
            );
            echo.await.unwrap();
        }
        let port = free_udp_port();
        let echo = spawn_udp_echo_count(port, 3).await;
        let packets: &[&[u8]] = &[b"first", &[0x52; 1600], b"last"];
        assert_eq!(
            socks5_udp_echo_sequence(socks, port, packets).await,
            packets
        );
        echo.await.unwrap();
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{carrier} relay timeout: {}",
            process
                .as_ref()
                .map(ExternalProcess::logs)
                .unwrap_or_default()
        )
    });
    let first_target = prefixes.first().map_or(tunnel, |(port, _)| *port);
    wait_for("carrier packets traverse the first hop", || {
        first.active_sessions().iter().any(|session| {
            session.port == first_target && session.network == zero_core::Network::Udp
        }) || first.completed_sessions().iter().any(|session| {
            session.port == first_target && session.network == zero_core::Network::Udp
        })
    })
    .await;
    for (index, (_, node)) in prefixes.iter().enumerate() {
        let target = prefixes.get(index + 1).map_or(tunnel, |(port, _)| *port);
        wait_for(
            "each intermediate hop records its downstream UDP target",
            || {
                node.active_sessions().iter().any(|session| {
                    session.port == target && session.network == zero_core::Network::Udp
                }) || node.completed_sessions().iter().any(|session| {
                    session.port == target && session.network == zero_core::Network::Udp
                })
            },
        )
        .await;
    }
    let accounting = client.engine().clone();
    let before = accounting.stats_snapshot().udp_upstream;
    assert!(
        before.created_associations > 0,
        "packet-path associations were not counted"
    );
    assert_eq!(
        before.created_associations.checked_sub(
            before.closed_associations + before.idle_timeouts + before.dropped_associations
        ),
        Some(before.active_associations)
    );
    client.shutdown().await.unwrap();
    // mKCP retains its carrier while completing the reference's bounded
    // close exchange (up to 15 seconds draining plus 8 seconds terminating).
    // Listener readiness's three-second helper is not a carrier-close deadline.
    tokio::time::timeout(std::time::Duration::from_secs(25), async {
        while accounting.stats_snapshot().udp_upstream.active_associations != 0 {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "{carrier} retained packet-path associations after bounded close: {:?}",
            accounting.stats_snapshot().udp_upstream
        )
    });
    let after = accounting.stats_snapshot().udp_upstream;
    assert_eq!(after.created_associations, after.closed_associations);
    first.shutdown().await.unwrap();
    for (_, prefix) in prefixes {
        prefix.shutdown().await.unwrap();
    }
    if let Some(server) = server {
        server.shutdown().await.unwrap();
    }
}
