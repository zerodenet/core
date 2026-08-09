use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::*;
use tokio::net::TcpListener;

const PASSWORD: &str = "test-password";
static NEXT_TLS_DIR: AtomicUsize = AtomicUsize::new(1);

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "trojan"))]
async fn relays_udp_through_trojan_outbound() {
    exercise_trojan_udp_outbound(false).await;
}

#[tokio::test]
#[cfg(all(feature = "socks5", feature = "trojan"))]
async fn relays_udp_through_trojan_mux_outbound() {
    exercise_trojan_udp_outbound(true).await;
}

async fn exercise_trojan_udp_outbound(mux: bool) {
    let tls = test_tls_material();
    let echo_port = free_udp_port();
    let tcp_echo_port = free_port();
    let upstream_port = free_port();
    let outer_port = free_port();

    let echo_task = tokio::spawn(async move {
        let socket = UdpSocket::bind(("127.0.0.1", echo_port))
            .await
            .expect("bind udp echo");
        let mut buf = [0_u8; 1024];
        for _ in 0..2 {
            let (read, peer) = socket.recv_from(&mut buf).await.expect("recv udp");
            socket
                .send_to(&buf[..read], peer)
                .await
                .expect("send udp echo");
        }
    });
    let tcp_echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", tcp_echo_port))
            .await
            .expect("bind tcp echo");
        let (mut stream, _) = listener.accept().await.expect("accept tcp echo");
        let mut payload = [0_u8; 4];
        stream
            .read_exact(&mut payload)
            .await
            .expect("read tcp echo");
        stream.write_all(&payload).await.expect("write tcp echo");
    });

    let upstream_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "upstream-trojan-in",
                    "listen": {{ "address": "127.0.0.1", "port": {upstream_port} }},
                    "protocol": {{
                        "type": "trojan",
                        "password": "{PASSWORD}",
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
    let upstream_engine = Engine::new(upstream_config).expect("build upstream engine");
    let upstream_handle = spawn_engine(upstream_engine);

    wait_for_listener(upstream_port).await;

    let mux_config = if mux { r#", "mux_concurrency": 8"# } else { "" };

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
                    "tag": "trojan-udp-chain",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": {upstream_port},
                        "password": "{PASSWORD}",
                        "sni": "localhost",
                        "insecure": true{mux_config}
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "trojan-udp-chain" }}
            }}
        }}"#
    ))
    .expect("parse outer config");
    let outer_engine = Engine::new(outer_config).expect("build outer engine");
    let outer_probe = outer_engine.clone();
    let outer_handle = spawn_engine(outer_engine);

    wait_for_listener(outer_port).await;

    let mut tcp_client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect tcp client");
    tcp_client
        .write_all(&[0x05, 0x01, 0x00])
        .await
        .expect("write tcp auth");
    let mut tcp_auth = [0_u8; 2];
    tcp_client
        .read_exact(&mut tcp_auth)
        .await
        .expect("read tcp auth");
    assert_eq!(tcp_auth, [0x05, 0x00]);
    let mut connect = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    connect.extend_from_slice(&tcp_echo_port.to_be_bytes());
    tcp_client
        .write_all(&connect)
        .await
        .expect("write tcp connect");
    let mut tcp_response = [0_u8; 10];
    tcp_client
        .read_exact(&mut tcp_response)
        .await
        .expect("read tcp connect");
    assert_eq!(tcp_response[1], 0x00);
    tcp_client
        .write_all(b"tmux")
        .await
        .expect("write tcp payload");
    let mut tcp_echoed = [0_u8; 4];
    tcp_client
        .read_exact(&mut tcp_echoed)
        .await
        .expect("read tcp payload");
    assert_eq!(&tcp_echoed, b"tmux");

    let mut control = timeout(
        Duration::from_secs(3),
        TcpStream::connect(("127.0.0.1", outer_port)),
    )
    .await
    .expect("connect outer proxy timeout")
    .expect("connect outer proxy");
    timeout(
        Duration::from_secs(3),
        control.write_all(&[0x05, 0x01, 0x00]),
    )
    .await
    .expect("write auth timeout")
    .expect("write auth");

    let mut auth = [0_u8; 2];
    timeout(Duration::from_secs(3), control.read_exact(&mut auth))
        .await
        .expect("read auth timeout")
        .expect("read auth");
    assert_eq!(auth, [0x05, 0x00]);

    timeout(
        Duration::from_secs(3),
        control.write_all(&[
            0x05, 0x03, 0x00, 0x01, // udp associate + ipv4
            0, 0, 0, 0, 0x00, 0x00,
        ]),
    )
    .await
    .expect("write udp associate timeout")
    .expect("write udp associate");

    let mut response = [0_u8; 10];
    timeout(Duration::from_secs(3), control.read_exact(&mut response))
        .await
        .expect("read udp associate response timeout")
        .expect("read udp associate response");
    assert_eq!(response[1], 0x00);
    let relay_port = u16::from_be_bytes([response[8], response[9]]);

    let client = UdpSocket::bind(("127.0.0.1", 0))
        .await
        .expect("bind udp client");
    let packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"trup")
        .expect("build udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .expect("send udp packet");

    let mut buf = [0_u8; 1024];
    let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
        .await
        .expect("udp recv timeout")
        .expect("recv udp response");
    let response = parse_udp_packet(&buf[..read]).expect("parse udp response");

    assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(response.port, echo_port);
    assert_eq!(response.payload, b"trup");

    let packet = build_udp_packet(&Address::Ipv4([127, 0, 0, 1]), echo_port, b"tr2")
        .expect("build second udp packet");
    client
        .send_to(&packet, ("127.0.0.1", relay_port))
        .await
        .expect("send second udp packet");

    let (read, _) = timeout(Duration::from_secs(3), client.recv_from(&mut buf))
        .await
        .expect("second udp recv timeout")
        .expect("recv second udp response");
    let response = parse_udp_packet(&buf[..read]).expect("parse second udp response");

    assert_eq!(response.target, Address::Ipv4([127, 0, 0, 1]));
    assert_eq!(response.port, echo_port);
    assert_eq!(response.payload, b"tr2");

    wait_for("outer udp session to record trojan outbound", || {
        outer_probe.active_sessions().iter().any(|session| {
            session.network == zero_core::Network::Udp
                && session.outbound_tag.as_deref() == Some("trojan-udp-chain")
                && session.protocol == zero_core::ProtocolType::new("socks5")
                && session.bytes_up > 0
                && session.bytes_down > 0
        })
    })
    .await;

    drop(control);
    wait_for("outer udp trojan session to complete", || {
        outer_probe.completed_sessions().iter().any(|session| {
            session.network == zero_core::Network::Udp
                && session.outbound_tag.as_deref() == Some("trojan-udp-chain")
                && session.outcome.kind() == "chained_relayed"
                && session.bytes_up > 0
                && session.bytes_down > 0
        })
    })
    .await;

    timeout(Duration::from_secs(3), outer_handle.shutdown())
        .await
        .expect("shutdown outer engine timeout")
        .expect("shutdown outer engine");
    timeout(Duration::from_secs(3), upstream_handle.shutdown())
        .await
        .expect("shutdown upstream engine timeout")
        .expect("shutdown upstream engine");
    timeout(Duration::from_secs(3), echo_task)
        .await
        .expect("join echo timeout")
        .expect("join echo task");
    timeout(Duration::from_secs(3), tcp_echo_task)
        .await
        .expect("join tcp echo timeout")
        .expect("join tcp echo task");
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
        "zero-trojan-tls-{}-{}-{}",
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
