#![cfg(all(feature = "socks5", feature = "hysteria2"))]
#[path = "hysteria2_official_interop/bandwidth.rs"]
mod bandwidth;
#[path = "hysteria2_official_interop/bbr.rs"]
mod bbr;
#[path = "hysteria2_official_interop/impairment.rs"]
mod impairment;
mod support;

use base64::Engine as _;
use ring::digest::{digest, SHA256};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use support::interop::{
    socks5_tcp_echo, socks5_udp_echo, spawn_tcp_echo, spawn_udp_echo, ExternalProcess, TempMaterial,
};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};
use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout, Duration};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy;

/// HY2_BIN must be the official Hysteria application; use app/v2.12.2 for parity qualification.
async fn interop(zero_is_client: bool, udp: bool) {
    configured_interop(zero_is_client, udp, false).await;
}

async fn configured_interop(zero_is_client: bool, udp: bool, brutal: bool) {
    interop_case(zero_is_client, udp, brutal, false).await;
}

async fn interop_case(zero_is_client: bool, udp: bool, brutal: bool, shared: bool) {
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
    let (mut zero_config, mut official_config, mode) = if zero_is_client {
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
    if brutal {
        let protocol = if zero_is_client {
            &mut zero_config["outbounds"][0]["protocol"]
        } else {
            &mut zero_config["inbounds"][0]["protocol"]
        };
        // Zero keeps client-relative upload/download in bytes/sec on either side.
        protocol["up_bps"] = serde_json::json!(1_250_000);
        protocol["down_bps"] = serde_json::json!(2_500_000);
        protocol["transport"] = serde_json::json!({"quic":{"stream_receive_window":1048576,"connection_receive_window":4194304,"keep_alive_interval_secs":5,"disable_path_mtu_discovery":true}});
        official_config["bandwidth"] = serde_json::json!({"up":"20 Mbps","down":"10 Mbps"});
    }
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
    if shared {
        shared::mixed_traffic(socks_port).await;
        assert_eq!(
            official.logs().matches("client connected").count(),
            1,
            "mixed TCP/UDP must authenticate once: {}",
            official.logs()
        );
        proxy.shutdown().await.unwrap();
        official.kill();
        return;
    }
    let payload = (0..if udp {
        1600
    } else if brutal {
        65_536
    } else {
        128
    })
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

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn zero_to_official_brutal_tcp_and_udp() {
    configured_interop(true, false, true).await;
    configured_interop(true, true, true).await;
}
#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn official_to_zero_brutal_tcp_and_udp() {
    configured_interop(false, false, true).await;
    configured_interop(false, true, true).await;
}

#[tokio::test]
#[ignore = "requires HY2_BIN pointing to official Hysteria app/v2.12.2"]
async fn zero_to_official_salamander_ca_pin_and_actual_port_hopping() {
    support::interop::init_logs("zero_proxy=debug,zero_transport=debug,quinn_proto=info");
    let binary = std::env::var("HY2_BIN").expect("official HY2_BIN is required");
    let material = TempMaterial::new("hysteria2-official-node-security");
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert_path = material.path("cert.pem");
    let key_path = material.path("key.pem");
    std::fs::write(&cert_path, cert.cert.pem()).unwrap();
    std::fs::write(&key_path, cert.signing_key.serialize_pem()).unwrap();
    let pin = base64::engine::general_purpose::STANDARD
        .encode(digest(&SHA256, cert.cert.der().as_ref()).as_ref());
    let server_port = free_udp_port();
    let hop_ports = (0..5).map(|_| free_udp_port()).collect::<Vec<_>>();
    let mut forwarders = Vec::new();
    let mut counters = Vec::new();
    for port in &hop_ports {
        let counter = Arc::new(AtomicU64::new(0));
        forwarders.push(spawn_udp_forwarder(*port, server_port, counter.clone()).await);
        counters.push(counter);
    }

    let password = "official-hy2-node-security";
    let obfs_password = "official-salamander-mask";
    let official_config = serde_json::json!({
        "listen": format!("127.0.0.1:{server_port}"),
        "tls": {"cert": cert_path, "key": key_path},
        "auth": {"type": "password", "password": password},
        "obfs": {"type": "salamander", "salamander": {"password": obfs_password}}
    });
    let config_path = material.path("hysteria.json");
    std::fs::write(&config_path, official_config.to_string()).unwrap();
    let mut official = ExternalProcess::start(
        binary,
        &[
            "server",
            "--config",
            config_path.to_str().unwrap(),
            "--disable-update-check",
        ],
        &material,
        "hysteria",
    );
    sleep(Duration::from_millis(300)).await;

    let socks_port = free_port();
    let zero_config = serde_json::json!({
        "inbounds": [{
            "tag": "socks",
            "listen": {"address": "127.0.0.1", "port": socks_port},
            "protocol": {"type": "socks5"}
        }],
        "outbounds": [{
            "tag": "hy",
            "protocol": {
                "type": "hysteria2",
                "server": "127.0.0.1",
                "server_name": "localhost",
                "port": server_port,
                "password": password,
                "ca_cert_path": cert_path,
                "tls_options": {"pinned_peer_cert_sha256": [pin]},
                "transport": {
                    "obfs": {"type": "salamander", "password": obfs_password},
                    "udp_hop": {
                        "ports": hop_ports,
                        "interval_min_secs": 5,
                        "interval_max_secs": 5
                    },
                    "quic": {"keep_alive_interval_secs": 2}
                }
            }
        }],
        "route": {"rules": [], "final": {"type": "route", "outbound": "hy"}}
    });
    let zero =
        spawn_engine(Proxy::new(RuntimeConfig::parse(&zero_config.to_string()).unwrap()).unwrap());
    wait_for_listener(socks_port).await;

    for round in 0..5_u8 {
        let echo_port = free_port();
        let payload = vec![round; 128];
        let echo = spawn_tcp_echo(echo_port, payload.len()).await;
        let echoed = timeout(
            Duration::from_secs(10),
            socks5_tcp_echo(socks_port, echo_port, &payload),
        )
        .await
        .unwrap_or_else(|error| {
            panic!(
                "official Salamander/hopping round {round} timed out: {error}; {}",
                official.logs()
            )
        });
        assert_eq!(echoed, payload, "official logs={}", official.logs());
        timeout(Duration::from_secs(5), echo)
            .await
            .unwrap()
            .unwrap();
        if round != 4 {
            sleep(Duration::from_millis(5_200)).await;
        }
    }
    let used_ports = counters
        .iter()
        .filter(|counter| counter.load(Ordering::Relaxed) > 0)
        .count();
    assert!(
        used_ports >= 2,
        "port hopping never changed the remote UDP port; counters={:?}",
        counters
            .iter()
            .map(|counter| counter.load(Ordering::Relaxed))
            .collect::<Vec<_>>()
    );

    timeout(Duration::from_secs(5), zero.shutdown())
        .await
        .unwrap()
        .unwrap();
    official.kill();
    for forwarder in forwarders {
        forwarder.abort();
    }
}

async fn spawn_udp_forwarder(
    listen_port: u16,
    server_port: u16,
    counter: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<()> {
    let front = UdpSocket::bind(("127.0.0.1", listen_port)).await.unwrap();
    let back = UdpSocket::bind(("127.0.0.1", 0)).await.unwrap();
    let server = std::net::SocketAddr::from(([127, 0, 0, 1], server_port));
    tokio::spawn(async move {
        let mut front_buffer = vec![0_u8; 65_535];
        let mut back_buffer = vec![0_u8; 65_535];
        let mut client = None;
        loop {
            tokio::select! {
                received = front.recv_from(&mut front_buffer) => {
                    let (length, source) = received.unwrap();
                    client = Some(source);
                    counter.fetch_add(1, Ordering::Relaxed);
                    back.send_to(&front_buffer[..length], server).await.unwrap();
                }
                received = back.recv_from(&mut back_buffer) => {
                    let (length, _) = received.unwrap();
                    if let Some(client) = client {
                        front.send_to(&back_buffer[..length], client).await.unwrap();
                    }
                }
            }
        }
    })
}

#[path = "hysteria2_official_interop/windows.rs"]
mod windows;

#[path = "hysteria2_official_interop/shared.rs"]
mod shared;

#[path = "hysteria2_official_interop/website.rs"]
mod website;
mod website_support;
