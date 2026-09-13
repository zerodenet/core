#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for, wait_for_listener};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

#[tokio::test]
async fn xhttp_relay_prefix_carries_all_modes_rotated_clients_and_separate_downloads() {
    for tls in [false, true] {
        for mode in ["packet-up", "stream-up", "stream-one", "auto"] {
            relay_case(mode, tls, false).await;
        }
    }
}
#[tokio::test]
async fn xhttp_intermediate_hop_keeps_nested_downloads_and_udp_inside_the_chain() {
    for tls in [false, true] {
        relay_case("stream-up", tls, true).await;
    }
}
async fn relay_case(mode: &str, tls: bool, nested: bool) {
    let first_port = free_port();
    let final_port = free_port();
    let outer_port = free_port();
    let download_port = free_port();
    let middle_port = free_port();
    let material = TempMaterial::new("xhttp-relay");
    let first = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
        "inbounds":[{"tag":"first","listen":{"address":"127.0.0.1","port":first_port},"protocol":{"type":"socks5"}}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap());
    wait_for_listener(first_port).await;
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", download_port))
        .await
        .unwrap();
    let forwarder = tokio::spawn(async move {
        let mut jobs = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => {
                    let (mut incoming, _) = accepted.unwrap();
                    jobs.spawn(async move {
                        let mut outgoing = tokio::net::TcpStream::connect(("127.0.0.1", final_port)).await.unwrap();
                        let _ = tokio::io::copy_bidirectional(&mut incoming, &mut outgoing).await;
                    });
                }
                _ = jobs.join_next(), if !jobs.is_empty() => {},
            }
        }
    });
    let mut inbound = json!({"type":"vless","users":[{"id":"relay-user"}],"split_http":{"path":"/relay/","mode":"auto"}});
    let mut outbound = json!({"type":"vless","server":"127.0.0.1","port":final_port,"id":"relay-user","split_http":{"path":"/relay/","mode":mode,"sc_max_each_post_bytes":8192,"sc_min_posts_interval_ms":1,"xmux":{"max_connections":1,"c_max_reuse_times":3,"h_max_request_times":4}}});
    if tls {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert_path = material.path("cert.pem");
        let key_path = material.path("key.pem");
        std::fs::write(&cert_path, cert.cert.pem()).unwrap();
        std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
        inbound["tls"] = json!({"cert_path":cert_path,"key_path":key_path});
        outbound["tls"] = json!({"server_name":"localhost","ca_cert_path":cert_path});
    }
    let separate = mode != "stream-one";
    if separate {
        outbound["split_http"]["download_settings"] = json!({"server":"127.0.0.1","port":download_port,"tls":outbound.get("tls").cloned(),"split_http":{"path":"/relay/","xmux":{"max_connections":1}}});
    }
    let final_hop = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
        "inbounds":[{"tag":"final","listen":{"address":"127.0.0.1","port":final_port},"protocol":inbound}],
        "route":{"rules":[],"final":{"type":"direct"}}
    }).to_string()).unwrap()).unwrap());
    wait_for_listener(final_port).await;
    let middle = if nested {
        let mut protocol = json!({"type":"vless","users":[{"id":"middle-user"}],"split_http":{"path":"/middle/","mode":"auto"}});
        if tls {
            protocol["tls"] = inbound_tls(&material);
        }
        let server = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
            "inbounds":[{"tag":"middle","listen":{"address":"127.0.0.1","port":middle_port},"protocol":protocol}],
            "route":{"rules":[],"final":{"type":"direct"}}
        }).to_string()).unwrap()).unwrap());
        wait_for_listener(middle_port).await;
        Some(server)
    } else {
        None
    };
    let mut outbounds = vec![
        json!({"tag":"first","protocol":{"type":"socks5","server":"127.0.0.1","port":first_port}}),
        json!({"tag":"final","protocol":outbound}),
    ];
    let chain = if nested {
        let mut protocol = json!({"type":"vless","server":"127.0.0.1","port":middle_port,"id":"middle-user","split_http":{"path":"/middle/","mode":"packet-up","sc_max_each_post_bytes":8192,"sc_min_posts_interval_ms":1,"xmux":{"max_connections":1,"h_max_request_times":4}}});
        if tls {
            protocol["tls"] =
                json!({"server_name":"localhost","ca_cert_path":material.path("cert.pem")});
        }
        outbounds.push(json!({"tag":"middle","protocol":protocol}));
        vec!["first", "middle", "final"]
    } else {
        vec!["first", "final"]
    };
    let outer = spawn_engine(Proxy::new(RuntimeConfig::parse(&json!({
        "inbounds":[{"tag":"client","listen":{"address":"127.0.0.1","port":outer_port},"protocol":{"type":"socks5"}}],
        "outbounds":outbounds,
        "outbound_groups":[{"tag":"chain","type":"relay","proxies":chain}],
        "route":{"rules":[],"final":{"type":"route","outbound":"chain"}}
    }).to_string()).unwrap()).unwrap());
    wait_for_listener(outer_port).await;
    for sequence in 0..4u8 {
        let echo_port = free_port();
        let payload = vec![sequence; 65_537];
        let echo = spawn_tcp_echo(echo_port, payload.len()).await;
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            socks5_tcp_echo_once(outer_port, echo_port, &payload),
        )
        .await
        .expect("relay TCP deadline")
        .expect("relay TCP exchange");
        assert_eq!(result, payload, "mode={mode} tls={tls}");
        echo.await.unwrap();
    }
    let udp_port = free_udp_port();
    let udp_echo = spawn_udp_echo_count(udp_port, 2).await;
    let packets: &[&[u8]] = &[b"first-relay-packet", b"second-relay-packet"];
    let received = socks5_udp_echo_sequence(outer_port, udp_port, packets).await;
    assert_eq!(received, packets, "UDP mode={mode} tls={tls}");
    udp_echo.await.unwrap();
    for port in if separate {
        vec![final_port, download_port]
    } else {
        vec![final_port]
    } {
        wait_for(
            "both carrier endpoints traverse the first relay hop",
            || {
                middle
                    .as_ref()
                    .unwrap_or(&first)
                    .active_sessions()
                    .iter()
                    .any(|s| s.port == port)
                    || middle
                        .as_ref()
                        .unwrap_or(&first)
                        .completed_sessions()
                        .iter()
                        .any(|s| s.port == port)
            },
        )
        .await;
    }
    if nested {
        assert!(
            first
                .active_sessions()
                .iter()
                .any(|s| s.port == middle_port)
                || first
                    .completed_sessions()
                    .iter()
                    .any(|s| s.port == middle_port)
        );
    }
    outer.shutdown().await.unwrap();
    if let Some(middle) = middle {
        middle.shutdown().await.unwrap();
    }
    first.shutdown().await.unwrap();
    forwarder.abort();
    final_hop.shutdown().await.unwrap();
}

fn inbound_tls(material: &TempMaterial) -> serde_json::Value {
    json!({"cert_path":material.path("cert.pem"),"key_path":material.path("key.pem")})
}
