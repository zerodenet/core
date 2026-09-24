#![cfg(all(feature = "wireguard", feature = "socks5"))]

mod support;

use std::net::{IpAddr, Ipv4Addr};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use boringtun::x25519::{PublicKey, StaticSecret};
use tokio::time::{sleep, timeout, Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy;

use support::interop::{init_logs, require_env, socks5_udp_echo_to, TempMaterial, XrayProcess};
use support::{free_port, free_udp_port, spawn_engine, wait_for_listener};

fn public_key(secret: u8) -> String {
    STANDARD.encode(PublicKey::from(&StaticSecret::from([secret; 32])).as_bytes())
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn zero_wireguard_udp_outbound_interops_with_xray_wireguard_inbound() {
    init_logs("wireguard=debug");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let material = TempMaterial::new("zero-xray-wireguard-udp");
    let xray_port = free_udp_port();
    let zero_socks_port = free_port();
    let host_ip = non_loopback_host_ipv4();
    let echo_socket = UdpSocket::bind((host_ip, 0)).await.expect("bind UDP echo");
    let echo_port = echo_socket.local_addr().unwrap().port();
    let client_private = STANDARD.encode([1_u8; 32]);
    let server_private = STANDARD.encode([2_u8; 32]);
    let server_public = public_key(2);
    let client_public = public_key(1);

    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        serde_json::json!({
            "log": {"loglevel": "warning"},
            "inbounds": [{
                "tag": "wg-in",
                "listen": "127.0.0.1",
                "port": xray_port,
                "protocol": "wireguard",
                "settings": {
                    "secretKey": server_private,
                    "peers": [{
                        "publicKey": client_public,
                        "allowedIPs": ["10.0.0.2/32"]
                    }],
                    "mtu": 1420
                }
            }],
            "outbounds": [{"protocol": "freedom"}]
        })
        .to_string(),
    )
    .expect("write Xray config");
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    sleep(Duration::from_millis(200)).await;

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {zero_socks_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [{{
                "tag": "wg-out",
                "protocol": {{
                    "type": "wireguard",
                    "private_key": "{client_private}",
                    "addresses": ["10.0.0.2/32"],
                    "mtu": 1420,
                    "peers": [{{
                        "public_key": "{server_public}",
                        "endpoint": "127.0.0.1:{xray_port}",
                        "allowed_ips": ["0.0.0.0/0"],
                        "keepalive_secs": 0
                    }}]
                }}
            }}],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "wg-out" }} }}
        }}"#
    ))
    .expect("parse Zero WireGuard config");
    let zero = spawn_engine(Proxy::new(zero_config).expect("build Zero proxy"));
    wait_for_listener(zero_socks_port).await;

    let payload = b"zero-wireguard-xray-udp";
    let echo = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        let (size, sender) = echo_socket
            .recv_from(&mut buffer)
            .await
            .expect("UDP echo recv");
        echo_socket
            .send_to(&buffer[..size], sender)
            .await
            .expect("UDP echo send");
    });
    let received = timeout(
        Duration::from_secs(15),
        socks5_udp_echo_to(
            zero_socks_port,
            Address::Ipv4(host_ip.octets()),
            echo_port,
            payload,
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "WireGuard UDP interop timed out: {error}; xray={}",
            xray.logs()
        )
    });
    assert_eq!(received, payload, "xray={}", xray.logs());

    zero.shutdown().await.expect("shutdown Zero");
    xray.kill();
    echo.await.expect("echo task");
}

fn non_loopback_host_ipv4() -> Ipv4Addr {
    let socket = std::net::UdpSocket::bind("0.0.0.0:0").expect("route probe socket");
    socket
        .connect("192.0.2.1:9")
        .expect("route to test-net is required for Xray interop");
    let IpAddr::V4(address) = socket.local_addr().expect("route probe local address").ip() else {
        panic!("IPv4 route required for Xray interop");
    };
    assert!(!address.is_loopback() && !address.is_unspecified());
    address
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn zero_wireguard_tcp_outbound_interops_with_xray_wireguard_inbound() {
    init_logs("wireguard=debug");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let material = TempMaterial::new("zero-xray-wireguard-tcp");
    let xray_port = free_udp_port();
    let zero_socks_port = free_port();
    let host_ip = non_loopback_host_ipv4();
    let echo = TcpListener::bind((host_ip, 0))
        .await
        .expect("bind TCP echo");
    let echo_port = echo.local_addr().unwrap().port();
    let client_private = STANDARD.encode([1_u8; 32]);
    let server_private = STANDARD.encode([2_u8; 32]);
    let server_public = public_key(2);
    let client_public = public_key(1);
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        serde_json::json!({
            "log": {"loglevel": "warning"},
            "inbounds": [{
                "tag": "wg-in", "listen": "127.0.0.1", "port": xray_port,
                "protocol": "wireguard",
                "settings": {
                    "secretKey": server_private,
                    "peers": [{"publicKey": client_public, "allowedIPs": ["10.0.0.2/32"]}],
                    "mtu": 1420
                }
            }],
            "outbounds": [{"protocol": "freedom"}]
        })
        .to_string(),
    )
    .expect("write Xray config");
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    sleep(Duration::from_millis(200)).await;
    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {zero_socks_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [{{
                "tag": "wg-out",
                "protocol": {{
                    "type": "wireguard",
                    "private_key": "{client_private}",
                    "addresses": ["10.0.0.2/32"],
                    "mtu": 1420,
                    "peers": [{{
                        "public_key": "{server_public}",
                        "endpoint": "127.0.0.1:{xray_port}",
                        "allowed_ips": ["0.0.0.0/0"],
                        "keepalive_secs": 0
                    }}]
                }}
            }}],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "wg-out" }} }}
        }}"#
    ))
    .expect("parse Zero WireGuard config");
    let zero = spawn_engine(Proxy::new(zero_config).expect("build Zero proxy"));
    wait_for_listener(zero_socks_port).await;
    let payload = b"zero-wireguard-xray-tcp".repeat(4_096);
    let echo_size = payload.len();
    let echo_task = tokio::spawn(async move {
        let (mut stream, _) = echo.accept().await.expect("accept TCP echo");
        let mut buffer = vec![0_u8; echo_size];
        stream.read_exact(&mut buffer).await.expect("read TCP echo");
        stream.write_all(&buffer).await.expect("write TCP echo");
    });
    let roundtrip = async {
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, zero_socks_port))
            .await
            .expect("connect SOCKS");
        stream.write_all(&[5, 1, 0]).await.expect("SOCKS greeting");
        let mut greeting = [0_u8; 2];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("SOCKS greeting response");
        assert_eq!(greeting, [5, 0]);
        let mut request = vec![5, 1, 0, 1];
        request.extend_from_slice(&host_ip.octets());
        request.extend_from_slice(&echo_port.to_be_bytes());
        stream.write_all(&request).await.expect("SOCKS connect");
        let mut response = [0_u8; 10];
        stream
            .read_exact(&mut response)
            .await
            .expect("SOCKS connect response");
        assert_eq!(response[1], 0, "SOCKS connect failed: {response:?}");
        stream.write_all(&payload).await.expect("write TCP payload");
        let mut received = vec![0_u8; payload.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("read TCP payload");
        assert_eq!(received, payload);
    };
    timeout(Duration::from_secs(20), roundtrip)
        .await
        .unwrap_or_else(|error| {
            panic!(
                "WireGuard TCP interop timed out: {error}; xray={}",
                xray.logs()
            )
        });
    zero.shutdown().await.expect("shutdown Zero");
    xray.kill();
    echo_task.await.expect("TCP echo task");
}

#[tokio::test]
#[ignore = "requires official XRAY_BIN v26.3.27"]
async fn xray_wireguard_outbound_interops_with_zero_wireguard_inbound() {
    init_logs("wireguard=debug");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let material = TempMaterial::new("xray-zero-wireguard-inbound");
    let zero_port = free_udp_port();
    let xray_socks_port = free_port();
    let host_ip = non_loopback_host_ipv4();
    let server_private = STANDARD.encode([81_u8; 32]);
    let client_private = STANDARD.encode([82_u8; 32]);
    let server_public = public_key(81);
    let client_public = public_key(82);

    let zero_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "wg-in", "listen": {{"address": "127.0.0.1", "port": {zero_port}}},
                "protocol": {{"type": "wireguard", "private_key": "{server_private}",
                    "peers": [{{"public_key": "{client_public}", "allowed_ips": ["10.0.0.2/32"]}}]}}
            }}],
            "outbounds": [{{"tag": "direct", "protocol": {{"type": "direct"}}}}],
            "route": {{"rules": [], "final": {{"type": "route", "outbound": "direct"}}}}
        }}"#
    ))
    .expect("parse Zero WireGuard inbound config");
    let zero = spawn_engine(Proxy::new(zero_config).expect("build Zero proxy"));
    sleep(Duration::from_millis(100)).await;

    let xray_config = material.path("xray-client.json");
    std::fs::write(
        &xray_config,
        serde_json::json!({
            "log": {"loglevel": "warning"},
            "inbounds": [{
                "tag": "socks-in", "listen": "127.0.0.1", "port": xray_socks_port,
                "protocol": "socks", "settings": {"auth": "noauth", "udp": true}
            }],
            "outbounds": [{
                "tag": "wg-out", "protocol": "wireguard",
                "settings": {
                    "secretKey": client_private,
                    "address": ["10.0.0.2/32"],
                    "peers": [{
                        "publicKey": server_public,
                        "endpoint": format!("127.0.0.1:{zero_port}"),
                        "allowedIPs": ["0.0.0.0/0"]
                    }],
                    "mtu": 1420,
                    "noKernelTun": true
                }
            }]
        })
        .to_string(),
    )
    .expect("write Xray client config");
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_socks_port).await;

    let udp_echo = UdpSocket::bind((host_ip, 0)).await.expect("bind UDP echo");
    let udp_port = udp_echo.local_addr().unwrap().port();
    let udp_task = tokio::spawn(async move {
        let mut buffer = [0_u8; 2048];
        let (size, source) = udp_echo
            .recv_from(&mut buffer)
            .await
            .expect("receive UDP echo");
        udp_echo
            .send_to(&buffer[..size], source)
            .await
            .expect("send UDP echo");
    });
    let payload = b"xray-to-zero-wireguard-udp";
    let received = timeout(
        Duration::from_secs(15),
        socks5_udp_echo_to(
            xray_socks_port,
            Address::Ipv4(host_ip.octets()),
            udp_port,
            payload,
        ),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "WireGuard inbound UDP timed out: {error}; xray={}",
            xray.logs()
        )
    });
    assert_eq!(received, payload, "xray={}", xray.logs());
    udp_task.await.expect("UDP echo task");

    let tcp_echo = TcpListener::bind((host_ip, 0))
        .await
        .expect("bind TCP echo");
    let tcp_port = tcp_echo.local_addr().unwrap().port();
    let tcp_payload = b"xray-to-zero-wireguard-tcp".repeat(256);
    let tcp_len = tcp_payload.len();
    let tcp_task = tokio::spawn(async move {
        let (mut stream, _) = tcp_echo.accept().await.expect("accept TCP echo");
        let mut data = vec![0; tcp_len];
        stream.read_exact(&mut data).await.expect("read TCP echo");
        stream.write_all(&data).await.expect("write TCP echo");
    });
    timeout(Duration::from_secs(20), async {
        let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, xray_socks_port))
            .await
            .expect("connect Xray SOCKS");
        stream.write_all(&[5, 1, 0]).await.expect("SOCKS greeting");
        let mut greeting = [0; 2];
        stream
            .read_exact(&mut greeting)
            .await
            .expect("SOCKS greeting response");
        assert_eq!(greeting, [5, 0]);
        let mut request = vec![5, 1, 0, 1];
        request.extend_from_slice(&host_ip.octets());
        request.extend_from_slice(&tcp_port.to_be_bytes());
        stream.write_all(&request).await.expect("SOCKS CONNECT");
        let mut response = [0; 10];
        stream
            .read_exact(&mut response)
            .await
            .expect("SOCKS CONNECT response");
        assert_eq!(response[1], 0, "SOCKS CONNECT failed: {response:?}");
        stream
            .write_all(&tcp_payload)
            .await
            .expect("write TCP payload");
        let mut received = vec![0; tcp_payload.len()];
        stream
            .read_exact(&mut received)
            .await
            .expect("read TCP payload");
        assert_eq!(received, tcp_payload);
    })
    .await
    .unwrap_or_else(|error| {
        panic!(
            "WireGuard inbound TCP timed out: {error}; xray={}",
            xray.logs()
        )
    });
    tcp_task.await.expect("TCP echo task");

    zero.shutdown().await.expect("shutdown Zero");
    xray.kill();
}
