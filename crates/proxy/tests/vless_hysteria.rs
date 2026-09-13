#![cfg(all(feature = "socks5", feature = "vless"))]
mod support;
use serde_json::json;
use support::interop::{
    socks5_tcp_echo_once, socks5_udp_echo_sequence, spawn_tcp_echo, spawn_udp_echo_count,
    ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::time::{timeout, Duration};
const ID: &str = "01234567-89ab-cdef-0123-456789abcdef";
#[tokio::test]
async fn hysteria_carries_native_vless_tcp_and_udp() {
    run(None, false).await;
}
#[tokio::test]
async fn hysteria_interoperates_with_pinned_official_client_and_server() {
    let Some(bin) = support::interop::require_env("XRAY_BIN") else {
        return;
    };
    run(Some(bin.clone()), false).await;
    run(Some(bin), true).await;
}
#[tokio::test]
async fn hopping_keeps_authenticated_tcp_and_udp_across_port_override_and_socket_rotation() {
    run_hopping(None, false, true).await;
    if let Some(bin) = support::interop::require_env("XRAY_BIN") {
        run_hopping(Some(bin.clone()), false, true).await;
        run_hopping(Some(bin), true, true).await;
    }
}
async fn run(official: Option<String>, official_server: bool) {
    run_hopping(official, official_server, false).await;
}
async fn run_hopping(official: Option<String>, official_server: bool, hopping: bool) {
    support::interop::init_logs("vless=debug,zero_transport=debug");
    let tunnel = free_udp_port();
    let socks = free_port();
    let ready = free_port();
    let target_tcp = free_port();
    let target_udp = free_udp_port();
    let material = TempMaterial::new("vless-hysteria");
    let tls = material.tls();
    let mut processes = Vec::new();
    let mut engines = Vec::new();
    let server = json!({"inbounds":[
        {"tag":"tunnel","listen":{"address":"127.0.0.1","port":tunnel},"protocol":{"type":"vless","users":[{"id":ID}],"hysteria":{"auth":"carrier-secret"},"quic":{"cert_path":tls.cert_path,"key_path":tls.key_path}}},
        {"tag":"ready","listen":{"address":"127.0.0.1","port":ready},"protocol":{"type":"socks5"}}
    ],"route":{"final":{"type":"direct"}}});
    let mut client = json!({"inbounds":[{"tag":"socks","listen":{"address":"127.0.0.1","port":socks},"protocol":{"type":"socks5"}}],
        "outbounds":[{"tag":"node","protocol":{"type":"vless","server":"127.0.0.1","port":tunnel,"id":ID,"hysteria":{"auth":"carrier-secret"},"quic":{"server_name":"localhost","ca_cert_path":tls.cert_path}}}],
        "route":{"final":{"type":"route","outbound":"node"}}});
    let xray_server = json!({"log":{"loglevel":"warning"},"inbounds":[
        {"port":tunnel,"listen":"127.0.0.1","protocol":"vless","settings":{"decryption":"none","clients":[{"id":ID}]},"streamSettings":{"network":"hysteria","hysteriaSettings":{"version":2,"auth":"carrier-secret"},"security":"tls","tlsSettings":{"alpn":["h3"],"certificates":[{"certificateFile":tls.cert_path,"keyFile":tls.key_path}]}}},
        {"port":ready,"listen":"127.0.0.1","protocol":"socks"}],"outbounds":[{"protocol":"freedom"}]});
    let mut xray_client = json!({"log":{"loglevel":"warning"},"inbounds":[{"port":socks,"listen":"127.0.0.1","protocol":"socks","settings":{"udp":true}}],
        "outbounds":[{"protocol":"vless","settings":{"address":"127.0.0.1","port":tunnel,"id":ID,"encryption":"none"},"streamSettings":{"network":"hysteria","hysteriaSettings":{"version":2,"auth":"carrier-secret"},"security":"tls","tlsSettings":{"alpn":["h3"],"serverName":"localhost","pinnedPeerCertSha256":tls.cert_sha256_hex}}}]});
    if hopping {
        let unused_port = free_udp_port();
        client["outbounds"][0]["protocol"]["port"] = json!(unused_port);
        client["outbounds"][0]["protocol"]["hysteria"]["quic_parameters"] =
            json!({"udp_hop":{"ports":[tunnel],"interval_min_secs":5,"interval_max_secs":5}});
        xray_client["outbounds"][0]["settings"]["port"] = json!(unused_port);
        xray_client["outbounds"][0]["streamSettings"]["finalmask"] =
            json!({"quicParams":{"udpHop":{"ports":tunnel.to_string(),"interval":5}}});
    }
    for (name, native, reference, is_official) in [
        (
            "server",
            server,
            xray_server,
            official.is_some() && official_server,
        ),
        (
            "client",
            client,
            xray_client,
            official.is_some() && !official_server,
        ),
    ] {
        if is_official {
            let path = material.path(&format!("{name}.json"));
            std::fs::write(&path, reference.to_string()).unwrap();
            processes.push(ExternalProcess::start(
                official.clone().unwrap(),
                &["run", "-c", path.to_str().unwrap()],
                &material,
                name,
            ));
        } else {
            engines.push(spawn_engine(
                zero_proxy::Proxy::new(
                    zero_config::RuntimeConfig::parse(&native.to_string()).unwrap(),
                )
                .unwrap(),
            ));
        }
    }
    wait_for_listener(ready).await;
    wait_for_listener(socks).await;
    timeout(Duration::from_secs(20), async {
        let payload = vec![0x37; 131073];
        let echo = spawn_tcp_echo(target_tcp, payload.len()).await;
        assert_eq!(
            socks5_tcp_echo_once(socks, target_tcp, &payload)
                .await
                .unwrap(),
            payload
        );
        echo.await.unwrap();
        if hopping {
            tokio::time::sleep(Duration::from_secs(6)).await;
            let next_target = free_port();
            let echo = spawn_tcp_echo(next_target, payload.len()).await;
            assert_eq!(
                socks5_tcp_echo_once(socks, next_target, &payload)
                    .await
                    .unwrap(),
                payload
            );
            echo.await.unwrap();
        }
        let echo = spawn_udp_echo_count(target_udp, 3).await;
        let packets: &[&[u8]] = &[b"first", &[0x72; 1600], b"last"];
        assert_eq!(
            socks5_udp_echo_sequence(socks, target_udp, packets).await,
            packets
                .iter()
                .map(|packet| packet.to_vec())
                .collect::<Vec<_>>()
        );
        echo.await.unwrap();
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "Hysteria transfer timed out: {}",
            processes
                .iter()
                .map(ExternalProcess::logs)
                .collect::<Vec<_>>()
                .join("\n")
        )
    });
    for engine in engines {
        engine.shutdown().await.unwrap();
    }
}
