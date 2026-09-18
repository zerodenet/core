#![cfg(all(feature = "socks5", feature = "trojan"))]

mod support;

use support::interop::{
    socks5_tcp_echo, socks5_udp_echo, spawn_tcp_echo, spawn_udp_echo, ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

const PASSWORD: &str = "official-trojan-go-password";

#[tokio::test]
#[ignore = "requires TROJAN_GO_BIN pointing to official trojan-go v0.10.6"]
async fn zero_client_reaches_official_trojan_go_server_over_tcp_and_udp() {
    let binary = std::env::var("TROJAN_GO_BIN").expect("official TROJAN_GO_BIN is required");
    let material = TempMaterial::new("zero-trojan-go-server");
    let tls = material.tls();
    let server_port = free_port();
    let socks_port = free_port();
    let config_path = material.path("trojan-go-server.json");
    let official_config = serde_json::json!({
        "run_type": "server",
        "local_addr": "127.0.0.1",
        "local_port": server_port,
        "remote_addr": "127.0.0.1",
        "remote_port": 1,
        "password": [PASSWORD],
        "disable_http_check": true,
        "ssl": {
            "cert": tls.cert_path,
            "key": tls.key_path,
            "sni": "localhost",
            "verify_hostname": false
        },
        "mux": {"enabled": false}
    });
    std::fs::write(&config_path, official_config.to_string()).unwrap();
    let mut official = ExternalProcess::start(
        binary,
        &["-config", config_path.to_str().unwrap()],
        &material,
        "trojan-go",
    );
    wait_for_listener(server_port).await;

    let zero_config = serde_json::json!({
        "inbounds": [{
            "tag": "socks",
            "listen": {"address": "127.0.0.1", "port": socks_port},
            "protocol": {"type": "socks5"}
        }],
        "outbounds": [{
            "tag": "trojan",
            "protocol": {
                "type": "trojan",
                "server": "127.0.0.1",
                "port": server_port,
                "password": PASSWORD,
                "sni": "localhost",
                "insecure": true
            }
        }],
        "route": {"rules": [], "final": {"type": "route", "outbound": "trojan"}}
    });
    let zero =
        spawn_engine(Proxy::new(RuntimeConfig::parse(&zero_config.to_string()).unwrap()).unwrap());
    wait_for_listener(socks_port).await;
    exercise_tcp_udp(socks_port, || official.logs()).await;
    zero.shutdown().await.unwrap();
    official.kill();
}

#[tokio::test]
#[ignore = "requires TROJAN_GO_BIN pointing to official trojan-go v0.10.6"]
async fn official_trojan_go_client_reaches_zero_server_over_tcp_and_udp() {
    let binary = std::env::var("TROJAN_GO_BIN").expect("official TROJAN_GO_BIN is required");
    let material = TempMaterial::new("trojan-go-zero-server");
    let tls = material.tls();
    let server_port = free_port();
    let socks_port = free_port();
    let zero_config = serde_json::json!({
        "inbounds": [{
            "tag": "trojan",
            "listen": {"address": "127.0.0.1", "port": server_port},
            "protocol": {
                "type": "trojan",
                "password": PASSWORD,
                "tls": {"cert_path": tls.cert_path, "key_path": tls.key_path}
            }
        }],
        "outbounds": [],
        "route": {"rules": [], "final": {"type": "direct"}}
    });
    let zero =
        spawn_engine(Proxy::new(RuntimeConfig::parse(&zero_config.to_string()).unwrap()).unwrap());
    wait_for_listener(server_port).await;

    let config_path = material.path("trojan-go-client.json");
    let official_config = serde_json::json!({
        "run_type": "client",
        "local_addr": "127.0.0.1",
        "local_port": socks_port,
        "remote_addr": "127.0.0.1",
        "remote_port": server_port,
        "password": [PASSWORD],
        "ssl": {"verify": false, "verify_hostname": false, "sni": "localhost"},
        "mux": {"enabled": false}
    });
    std::fs::write(&config_path, official_config.to_string()).unwrap();
    let mut official = ExternalProcess::start(
        binary,
        &["-config", config_path.to_str().unwrap()],
        &material,
        "trojan-go",
    );
    wait_for_listener(socks_port).await;
    exercise_tcp_udp(socks_port, || official.logs()).await;
    official.kill();
    zero.shutdown().await.unwrap();
}

async fn exercise_tcp_udp(proxy_port: u16, logs: impl Fn() -> String) {
    let tcp_payload = b"trojan-go-tcp";
    let tcp_port = free_port();
    let tcp_echo = spawn_tcp_echo(tcp_port, tcp_payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo(proxy_port, tcp_port, tcp_payload),
    )
    .await
    .unwrap_or_else(|error| panic!("Trojan-Go TCP timed out: {error}; {}", logs()));
    assert_eq!(echoed, tcp_payload, "{}", logs());
    timeout(Duration::from_secs(5), tcp_echo)
        .await
        .unwrap()
        .unwrap();

    let udp_payload = b"trojan-go-udp";
    let udp_port = free_udp_port();
    let udp_echo = spawn_udp_echo(udp_port, udp_payload.len()).await;
    let echoed = timeout(
        Duration::from_secs(10),
        socks5_udp_echo(proxy_port, udp_port, udp_payload),
    )
    .await
    .unwrap_or_else(|error| panic!("Trojan-Go UDP timed out: {error}; {}", logs()));
    assert_eq!(echoed, udp_payload, "{}", logs());
    timeout(Duration::from_secs(5), udp_echo)
        .await
        .unwrap()
        .unwrap();
}
