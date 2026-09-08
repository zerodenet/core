#![cfg(all(feature = "socks5", feature = "hysteria2"))]
mod support;

use support::interop::{
    socks5_tcp_echo, socks5_udp_echo, spawn_tcp_echo, spawn_udp_echo, ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::time::{sleep, timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

/// HY2_BIN must be the official Hysteria application; use app/v2.12.2 for parity qualification.
async fn interop(zero_is_client: bool, udp: bool) {
    support::interop::init_logs("zero_proxy=debug,zero_transport=debug,quinn_proto=info");
    let binary = std::env::var("HY2_BIN").expect("HY2_BIN must point to official Hysteria");
    let material = TempMaterial::new("hysteria2-official-interop");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = material.path("cert.pem");
    let key_path = material.path("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let hy_port = free_udp_port();
    let socks_port = free_port();
    let password = "official-hy2-interop-password";
    let (zero_config, official_config, mode) = if zero_is_client {
        (
            serde_json::json!({
                "inbounds": [{"tag": "socks", "listen": {"address": "127.0.0.1", "port": socks_port}, "protocol": {"type": "socks5"}}],
                "outbounds": [{"tag": "hy", "protocol": {"type": "hysteria2", "server": "127.0.0.1", "server_name": "localhost", "port": hy_port, "password": password, "insecure": true}}],
                "route": {"rules": [], "final": {"type": "route", "outbound": "hy"}}
            }),
            serde_json::json!({
                "listen": format!("127.0.0.1:{hy_port}"),
                "tls": {"cert": cert_path, "key": key_path},
                "auth": {"type": "password", "password": password}
            }),
            "server",
        )
    } else {
        (
            serde_json::json!({
                "inbounds": [{"tag": "hy", "listen": {"address": "127.0.0.1", "port": hy_port}, "protocol": {"type": "hysteria2", "password": password, "cert_path": cert_path, "key_path": key_path}}],
                "outbounds": [], "route": {"rules": [], "final": {"type": "direct"}}
            }),
            serde_json::json!({
                "server": format!("127.0.0.1:{hy_port}"), "auth": password,
                "tls": {"insecure": true}, "socks5": {"listen": format!("127.0.0.1:{socks_port}")}
            }),
            "client",
        )
    };
    let proxy =
        spawn_engine(Proxy::new(RuntimeConfig::parse(&zero_config.to_string()).unwrap()).unwrap());
    let config_path = material.path("hysteria.json");
    std::fs::write(&config_path, official_config.to_string()).unwrap();
    let mut official = ExternalProcess::start(
        binary,
        &[
            mode,
            "--config",
            config_path.to_str().unwrap(),
            "--disable-update-check",
        ],
        &material,
        "hysteria",
    );
    wait_for_listener(socks_port).await;
    // UDP-only official servers have no TCP readiness endpoint.
    if zero_is_client {
        sleep(Duration::from_millis(300)).await;
    }
    let payload = (0..if udp { 1600 } else { 128 })
        .map(|i| (i % 251) as u8)
        .collect::<Vec<_>>();
    let target_port = if udp { free_udp_port() } else { free_port() };
    let echo = if udp {
        spawn_udp_echo(target_port, payload.len()).await
    } else {
        spawn_tcp_echo(target_port, payload.len()).await
    };
    let echoed = timeout(Duration::from_secs(10), async {
        if udp {
            socks5_udp_echo(socks_port, target_port, &payload).await
        } else {
            socks5_tcp_echo(socks_port, target_port, &payload).await
        }
    })
    .await
    .unwrap_or_else(|error| panic!("official interop timed out: {error}; {}", official.logs()));
    assert_eq!(echoed, payload, "{}", official.logs());
    timeout(Duration::from_secs(5), echo)
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(5), proxy.shutdown())
        .await
        .unwrap()
        .unwrap();
    official.kill();
}

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn zero_to_official_hysteria2_tcp() {
    interop(true, false).await;
}

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn zero_to_official_hysteria2_fragmented_udp() {
    interop(true, true).await;
}

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn official_to_zero_hysteria2_tcp() {
    interop(false, false).await;
}

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn official_to_zero_hysteria2_fragmented_udp() {
    interop(false, true).await;
}
