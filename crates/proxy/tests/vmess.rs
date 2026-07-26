#![cfg(all(feature = "socks5", feature = "vmess"))]

mod support;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::{timeout, Duration};
use zero_config::RuntimeConfig;
use zero_core::Address;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for, wait_for_listener};

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";
static NEXT_TLS_DIR: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn relays_tcp_through_vmess_tls_outbound_for_all_explicit_ciphers() {
    for cipher in ["aes-128-gcm", "chacha20-poly1305", "none", "zero"] {
        relays_tcp_through_vmess_tls_outbound(cipher).await;
    }
}

async fn relays_tcp_through_vmess_tls_outbound(cipher: &str) {
    relays_tcp_through_vmess_outbound(cipher, VmessTransport::RawTls, None, None).await;
}

#[tokio::test]
async fn relays_tcp_through_vmess_wss_outbound() {
    relays_tcp_through_vmess_outbound(
        "chacha20-poly1305",
        VmessTransport::Wss { path: "/vmess-wss" },
        None,
        None,
    )
    .await;
}

#[tokio::test]
async fn relays_tcp_through_vmess_grpc_outbound() {
    relays_tcp_through_vmess_outbound(
        "chacha20-poly1305",
        VmessTransport::Grpc {
            service_name: "/zero.vmess.grpc/Tun",
        },
        None,
        None,
    )
    .await;
}

#[tokio::test]
async fn relays_tcp_through_vmess_mux_outbound() {
    relays_tcp_through_vmess_outbound("chacha20-poly1305", VmessTransport::RawTls, Some(8), None)
        .await;
}

#[tokio::test]
async fn vmess_mux_idle_timeout_closes_the_upstream_carrier() {
    relays_tcp_through_vmess_outbound(
        "chacha20-poly1305",
        VmessTransport::RawTls,
        Some(8),
        Some(1),
    )
    .await;
}

#[tokio::test]
async fn relays_udp_through_vmess_tls_outbound() {
    let echo_port = support::free_udp_port();
    let upstream_port = free_port();
    let outer_port = free_port();
    let tls = test_tls_material();

    let echo_task = tokio::spawn(async move {
        let socket = UdpSocket::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind udp echo");
        let mut buf = [0_u8; 1024];
        let (read, peer) = socket.recv_from(&mut buf).await.expect("recv udp");
        socket
            .send_to(&buf[..read], peer)
            .await
            .expect("send udp echo");
    });

    let upstream_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "upstream-vmess-in",
                    "listen": {{ "address": "127.0.0.1", "port": {upstream_port} }},
                    "protocol": {{
                        "type": "vmess",
                        "users": [
                            {{
                                "id": "{USER_ID}",
                                "cipher": "chacha20-poly1305",
                                "principal_key": "node:vmess-udp"
                            }}
                        ],
                        "tls": {{
                            "cert_path": "{}",
                            "key_path": "{}"
                        }}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    ))
    .expect("parse upstream config");
    let upstream_handle =
        spawn_engine(Engine::new(upstream_config).expect("build upstream engine"));
    wait_for_listener(upstream_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "outer-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "vmess-udp-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {upstream_port},
                        "id": "{USER_ID}",
                        "cipher": "chacha20-poly1305",
                        "tls": {{
                            "server_name": "localhost",
                            "ca_cert_path": "{}"
                        }}
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "vmess-udp-out" }}
            }}
        }}"#,
        escape_json_path(&tls.cert_path),
    ))
    .expect("parse outer config");
    let outer_handle = spawn_engine(Engine::new(outer_config).expect("build outer engine"));
    wait_for_listener(outer_port).await;

    let mut control = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
    control
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write socks auth");

    let mut auth = [0_u8; 2];
    control.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    control
        .write_all(&[
            0x05, 0x03, 0x00, 0x01, // udp associate + ipv4
            0, 0, 0, 0, 0x00, 0x00,
        ])
        .await
        .expect("write udp associate");

    let mut response = [0_u8; 10];
    control
        .read_exact(&mut response)
        .await
        .expect("read udp associate response");
    assert_eq!(response[1], 0x00);
    let relay_port = u16::from_be_bytes([response[8], response[9]]);

    let client = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind udp client");
    let packet = support::build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"vudp")
        .expect("build socks5 udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .expect("send udp packet");

    let mut buf = [0_u8; 1024];
    let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
        .await
        .expect("udp recv timeout")
        .expect("recv udp response");
    let response = support::parse_udp_packet(&buf[..read]).expect("parse udp response");

    assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(response.port, echo_port);
    assert_eq!(response.payload, b"vudp");

    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    upstream_handle
        .shutdown()
        .await
        .expect("shutdown upstream engine");
    let _ = echo_task.await;
}

#[tokio::test]
async fn relays_udp_through_vmess_mux_outbound() {
    let echo_port = support::free_udp_port();
    let upstream_port = free_port();
    let outer_port = free_port();
    let tls = test_tls_material();

    let echo_task = tokio::spawn(async move {
        let socket = UdpSocket::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind udp echo");
        let mut buf = [0_u8; 1024];
        let (read, peer) = socket.recv_from(&mut buf).await.expect("recv udp");
        socket
            .send_to(&buf[..read], peer)
            .await
            .expect("send udp echo");
    });

    let upstream_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "upstream-vmess-in",
                    "listen": {{ "address": "127.0.0.1", "port": {upstream_port} }},
                    "protocol": {{
                        "type": "vmess",
                        "users": [
                            {{
                                "id": "{USER_ID}",
                                "cipher": "chacha20-poly1305",
                                "principal_key": "node:vmess-mux-udp"
                            }}
                        ],
                        "tls": {{
                            "cert_path": "{}",
                            "key_path": "{}"
                        }}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    ))
    .expect("parse upstream config");
    let upstream_handle =
        spawn_engine(Engine::new(upstream_config).expect("build upstream engine"));
    wait_for_listener(upstream_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "outer-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "vmess-mux-udp-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {upstream_port},
                        "id": "{USER_ID}",
                        "cipher": "chacha20-poly1305",
                        "mux_concurrency": 8,
                        "tls": {{
                            "server_name": "localhost",
                            "ca_cert_path": "{}"
                        }}
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "vmess-mux-udp-out" }}
            }}
        }}"#,
        escape_json_path(&tls.cert_path),
    ))
    .expect("parse outer config");
    let outer_handle = spawn_engine(Engine::new(outer_config).expect("build outer engine"));
    wait_for_listener(outer_port).await;

    let mut control = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
    control
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write socks auth");

    let mut auth = [0_u8; 2];
    control.read_exact(&mut auth).await.expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    control
        .write_all(&[
            0x05, 0x03, 0x00, 0x01, // udp associate + ipv4
            0, 0, 0, 0, 0x00, 0x00,
        ])
        .await
        .expect("write udp associate");

    let mut response = [0_u8; 10];
    control
        .read_exact(&mut response)
        .await
        .expect("read udp associate response");
    assert_eq!(response[1], 0x00);
    let relay_port = u16::from_be_bytes([response[8], response[9]]);

    let client = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind udp client");
    let packet = support::build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"vmux-udp")
        .expect("build socks5 udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .expect("send udp packet");

    let mut buf = [0_u8; 1024];
    let (read, _) = timeout(Duration::from_secs(5), client.recv_from(&mut buf))
        .await
        .expect("udp recv timeout")
        .expect("recv udp response");
    let response = support::parse_udp_packet(&buf[..read]).expect("parse udp response");

    assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(response.port, echo_port);
    assert_eq!(response.payload, b"vmux-udp");

    let upstream_active = upstream_handle.active_sessions();
    assert_eq!(upstream_active.len(), 1);
    assert_eq!(
        upstream_active[0]
            .auth
            .as_ref()
            .and_then(|auth| auth.principal_key.as_deref()),
        Some("node:vmess-mux-udp")
    );

    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    upstream_handle
        .shutdown()
        .await
        .expect("shutdown upstream engine");
    let _ = echo_task.await;
}

#[derive(Clone, Copy)]
enum VmessTransport<'a> {
    RawTls,
    Wss { path: &'a str },
    Grpc { service_name: &'a str },
}

impl VmessTransport<'_> {
    fn payload_prefix(self) -> &'static str {
        match self {
            Self::RawTls => "tls",
            Self::Wss { .. } => "wss",
            Self::Grpc { .. } => "grpc",
        }
    }

    fn config_suffix(self) -> String {
        match self {
            Self::RawTls => String::new(),
            Self::Wss { path } => format!(r#", "ws": {{ "path": "{path}" }}"#),
            Self::Grpc { service_name } => {
                format!(r#", "grpc": {{ "service_names": ["{service_name}"] }}"#)
            }
        }
    }
}

async fn relays_tcp_through_vmess_outbound(
    cipher: &str,
    transport: VmessTransport<'_>,
    mux_concurrency: Option<u32>,
    mux_idle_timeout_secs: Option<u64>,
) {
    let echo_port = free_port();
    let upstream_port = free_port();
    let outer_port = free_port();
    let tls = test_tls_material();
    let payload = format!("vmess:{}:{cipher}", transport.payload_prefix());
    let inbound_transport = transport.config_suffix();
    let outbound_transport = inbound_transport.clone();
    let mux_config = mux_concurrency
        .map(|value| format!(r#", "mux_concurrency": {value}"#))
        .unwrap_or_default();
    let mux_idle_config = mux_idle_timeout_secs
        .map(|value| format!(r#", "mux_idle_timeout_secs": {value}"#))
        .unwrap_or_default();

    let echo_task = spawn_echo_server(echo_port, payload.len()).await;

    let upstream_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "upstream-vmess-in",
                    "listen": {{ "address": "127.0.0.1", "port": {upstream_port} }},
                    "protocol": {{
                        "type": "vmess",
                        "users": [
                            {{
                                "id": "{USER_ID}",
                                "cipher": "{cipher}",
                                "principal_key": "node:vmess"
                            }}
                        ],
                        "tls": {{
                            "cert_path": "{}",
                            "key_path": "{}"
                        }}{inbound_transport}
                    }}
                }}
            ],
            "outbounds": [],
            "route": {{
                "rules": [],
                "final": {{ "type": "direct" }}
            }}
        }}"#,
        escape_json_path(&tls.cert_path),
        escape_json_path(&tls.key_path),
    ))
    .expect("parse upstream config");
    let upstream_handle =
        spawn_engine(Engine::new(upstream_config).expect("build upstream engine"));
    wait_for_listener(upstream_port).await;
    let carrier_proxy = if mux_idle_timeout_secs.is_some() {
        let proxy_port = free_port();
        Some((
            proxy_port,
            support::interop::TcpResetProxy::start(proxy_port, upstream_port).await,
        ))
    } else {
        None
    };
    let outbound_port = carrier_proxy
        .as_ref()
        .map_or(upstream_port, |(proxy_port, _)| *proxy_port);

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "outer-socks-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "socks5" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "vmess-out",
                    "protocol": {{
                        "type": "vmess",
                        "server": "127.0.0.1",
                        "port": {outbound_port},
                        "id": "{USER_ID}",
                        "cipher": "{cipher}"{mux_config}{mux_idle_config},
                        "tls": {{
                            "server_name": "localhost",
                            "ca_cert_path": "{}"
                        }}{outbound_transport}
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "vmess-out" }}
            }}
        }}"#,
        escape_json_path(&tls.cert_path),
    ))
    .expect("parse outer config");
    let outer_handle = spawn_engine(Engine::new(outer_config).expect("build outer engine"));
    wait_for_listener(outer_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
    client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write socks auth");

    let mut auth = [0_u8; 2];
    client.read_exact(&mut auth).await.expect("read socks auth");
    assert_eq!(auth, [0x05, 0x00]);

    client
        .write_all(&socks5_connect_ipv4([127, 0, 0, 1], echo_port))
        .await
        .expect("write socks request");

    let mut response = [0_u8; 10];
    timeout(Duration::from_secs(5), client.read_exact(&mut response))
        .await
        .expect("read socks response timeout")
        .expect("read socks response");
    assert_eq!(response[1], 0x00, "cipher: {cipher}");

    client
        .write_all(payload.as_bytes())
        .await
        .expect("write payload");
    let mut echoed = vec![0_u8; payload.len()];
    timeout(Duration::from_secs(5), client.read_exact(&mut echoed))
        .await
        .expect("read payload timeout")
        .expect("read payload");
    assert_eq!(echoed, payload.as_bytes(), "cipher: {cipher}");

    client.shutdown().await.expect("shutdown SOCKS client");
    drop(client);
    echo_task.await.expect("echo task");
    if let Some((_, proxy)) = &carrier_proxy {
        assert_eq!(proxy.accepted_connections(), 1);
        wait_for(
            "VMess MUX physical carrier to close after its idle timeout",
            || proxy.active_connections() == 0,
        )
        .await;
    }

    outer_handle
        .shutdown()
        .await
        .expect("shutdown outer engine");
    upstream_handle
        .shutdown()
        .await
        .expect("shutdown upstream engine");
    if let Some((_, proxy)) = carrier_proxy {
        proxy.shutdown().await;
    }
}

async fn spawn_echo_server(port: u16, payload_len: usize) -> tokio::task::JoinHandle<()> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind echo");
        let _ = ready_tx.send(());
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = vec![0_u8; payload_len];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });
    ready_rx.await.expect("echo server ready");
    task
}

fn socks5_connect_ipv4(address: [u8; 4], port: u16) -> Vec<u8> {
    let mut request = vec![0x05, 0x01, 0x00, 0x01];
    request.extend_from_slice(&address);
    request.extend_from_slice(&port.to_be_bytes());
    request
}

struct TestTlsMaterial {
    dir: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl Drop for TestTlsMaterial {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn test_tls_material() -> TestTlsMaterial {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate self-signed cert");
    let unique = NEXT_TLS_DIR.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "zero-vmess-tls-{}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos(),
        unique
    ));
    std::fs::create_dir_all(&dir).expect("create tls temp dir");
    let cert_path = dir.join("server.crt");
    let key_path = dir.join("server.key");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write cert");
    std::fs::write(&key_path, certified.signing_key.serialize_pem()).expect("write key");

    TestTlsMaterial {
        dir,
        cert_path,
        key_path,
    }
}

fn escape_json_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}
