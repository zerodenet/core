#![cfg(all(feature = "http", feature = "trojan"))]

mod support;

use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsAcceptor;
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::interop::TempMaterial;
use support::{free_port, spawn_engine, wait_for_listener};

const CMD_TCP: u8 = 0x01;
const ATYP_IPV4: u8 = 0x01;
const PASSWORD_HASH_LEN: usize = 56;

const PASSWORD: &str = "test-password";

#[derive(Clone, Copy)]
enum TrojanCarrier {
    WebSocket,
    Grpc,
}

impl TrojanCarrier {
    fn json(self) -> &'static str {
        match self {
            Self::WebSocket => r#""ws":{"path":"/trojan-ws"}"#,
            Self::Grpc => r#""grpc":{"service_names":["zero.trojan/Tun"]}"#,
        }
    }

    fn payload(self) -> &'static [u8; 4] {
        match self {
            Self::WebSocket => b"tws!",
            Self::Grpc => b"tgr!",
        }
    }
}

#[tokio::test]
async fn relays_tcp_through_trojan_websocket_outbound() {
    relay_tcp_through_trojan_carrier(TrojanCarrier::WebSocket).await;
}

#[tokio::test]
async fn relays_tcp_through_trojan_grpc_outbound() {
    relay_tcp_through_trojan_carrier(TrojanCarrier::Grpc).await;
}

async fn relay_tcp_through_trojan_carrier(carrier: TrojanCarrier) {
    let upstream_port = free_port();
    let outer_port = free_port();
    let target_port = free_port();
    let material = TempMaterial::new("zero-trojan-carrier");
    let tls = material.tls();
    let carrier_json = carrier.json();

    let echo_task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", target_port))
            .await
            .expect("bind echo target");
        let (mut stream, _) = listener.accept().await.expect("accept echo target");
        let mut payload = [0_u8; 4];
        stream
            .read_exact(&mut payload)
            .await
            .expect("read echo payload");
        stream
            .write_all(&payload)
            .await
            .expect("write echo payload");
    });

    let upstream_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{
                "tag":"trojan-carrier-in",
                "listen":{{"address":"127.0.0.1","port":{upstream_port}}},
                "protocol":{{
                    "type":"trojan",
                    "password":"{PASSWORD}",
                    "tls":{{"cert_path":"{}","key_path":"{}"}},
                    {carrier_json}
                }}
            }}],
            "outbounds":[],
            "route":{{"rules":[],"final":{{"type":"direct"}}}}
        }}"#,
        tls.cert_path.display(),
        tls.key_path.display(),
    ))
    .expect("parse upstream config");
    let upstream_handle = spawn_engine(Engine::new(upstream_config).expect("build upstream"));
    wait_for_listener(upstream_port).await;

    let outer_config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds":[{{
                "tag":"http-in",
                "listen":{{"address":"127.0.0.1","port":{outer_port}}},
                "protocol":{{"type":"http"}}
            }}],
            "outbounds":[{{
                "tag":"trojan-carrier-out",
                "protocol":{{
                    "type":"trojan",
                    "server":"127.0.0.1",
                    "port":{upstream_port},
                    "password":"{PASSWORD}",
                    "sni":"localhost",
                    "insecure":true,
                    {carrier_json}
                }}
            }}],
            "route":{{"rules":[],"final":{{"type":"route","outbound":"trojan-carrier-out"}}}}
        }}"#,
    ))
    .expect("parse outer config");
    let outer_handle = spawn_engine(Engine::new(outer_config).expect("build outer"));
    wait_for_listener(outer_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect outer proxy");
    client
        .write_all(
            format!(
                "CONNECT 127.0.0.1:{target_port} HTTP/1.1\r\nHost: 127.0.0.1:{target_port}\r\n\r\n"
            )
            .as_bytes(),
        )
        .await
        .expect("write connect request");
    let mut response = vec![0_u8; 39];
    client
        .read_exact(&mut response)
        .await
        .expect("read connect response");
    assert_eq!(&response, b"HTTP/1.1 200 Connection Established\r\n\r\n");

    client
        .write_all(carrier.payload())
        .await
        .expect("write carrier payload");
    let mut echoed = [0_u8; 4];
    client
        .read_exact(&mut echoed)
        .await
        .expect("read carrier payload");
    assert_eq!(&echoed, carrier.payload());

    outer_handle.shutdown().await.expect("shutdown outer");
    upstream_handle.shutdown().await.expect("shutdown upstream");
    echo_task.await.expect("echo task");
}

#[tokio::test]
async fn trojan_raw_outbound_does_not_negotiate_h2_alpn() {
    let upstream_port = free_port();
    let outer_port = free_port();
    let target_port = free_port();

    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [
                {{
                    "tag": "http-in",
                    "listen": {{ "address": "127.0.0.1", "port": {outer_port} }},
                    "protocol": {{ "type": "http" }}
                }}
            ],
            "outbounds": [
                {{
                    "tag": "trojan-chain",
                    "protocol": {{
                        "type": "trojan",
                        "server": "127.0.0.1",
                        "port": {upstream_port},
                        "password": "{PASSWORD}",
                        "sni": "localhost",
                        "insecure": true
                    }}
                }}
            ],
            "route": {{
                "rules": [],
                "final": {{ "type": "route", "outbound": "trojan-chain" }}
            }}
        }}"#
    ))
    .expect("parse engine config");
    let engine = Engine::new(config).expect("build engine");
    let engine_handle = spawn_engine(engine);

    wait_for_listener(outer_port).await;
    let upstream_task = spawn_trojan_tls_echo_server(upstream_port, target_port).await;

    let mut client = TcpStream::connect(("127.0.0.1", outer_port))
        .await
        .expect("connect proxy");
    let request = format!(
        "CONNECT 127.0.0.1:{target_port} HTTP/1.1\r\nHost: 127.0.0.1:{target_port}\r\n\r\n"
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write connect request");

    let mut response = vec![0_u8; 39];
    client
        .read_exact(&mut response)
        .await
        .expect("read connect response");
    assert_eq!(&response, b"HTTP/1.1 200 Connection Established\r\n\r\n");

    client.write_all(b"traw").await.expect("write payload");
    let mut echoed = [0_u8; 4];
    client.read_exact(&mut echoed).await.expect("read payload");
    assert_eq!(&echoed, b"traw");

    engine_handle.shutdown().await.expect("shutdown engine");
    upstream_task.await.expect("upstream task");
}

async fn spawn_trojan_tls_echo_server(
    port: u16,
    expected_target_port: u16,
) -> tokio::task::JoinHandle<()> {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate self-signed cert");
    let key_der = rustls::pki_types::PrivateKeyDer::from(
        rustls::pki_types::PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()),
    );
    let mut tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certified.cert.der().clone()], key_der)
        .expect("build server tls config");
    tls_config.alpn_protocols = vec![b"h2".to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(tls_config));

    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind trojan tls server");
        let _ = ready_tx.send(());

        let (stream, _) = listener.accept().await.expect("accept trojan tls");
        let mut stream = acceptor.accept(stream).await.expect("accept tls");
        assert!(stream.get_ref().1.alpn_protocol().is_none());

        let mut password = [0_u8; PASSWORD_HASH_LEN + 2];
        stream
            .read_exact(&mut password)
            .await
            .expect("read trojan password");
        assert_eq!(&password[PASSWORD_HASH_LEN..], b"\r\n");

        let mut header = [0_u8; 1];
        stream
            .read_exact(&mut header)
            .await
            .expect("read trojan command");
        assert_eq!(header[0], CMD_TCP);

        stream
            .read_exact(&mut header)
            .await
            .expect("read trojan address type");
        assert_eq!(header[0], ATYP_IPV4);

        let mut ipv4 = [0_u8; 4];
        stream
            .read_exact(&mut ipv4)
            .await
            .expect("read trojan address");
        assert_eq!(ipv4, [127, 0, 0, 1]);

        let mut port_bytes = [0_u8; 2];
        stream
            .read_exact(&mut port_bytes)
            .await
            .expect("read trojan port");
        assert_eq!(u16::from_be_bytes(port_bytes), expected_target_port);

        let mut crlf = [0_u8; 2];
        stream
            .read_exact(&mut crlf)
            .await
            .expect("read trojan crlf");
        assert_eq!(&crlf, b"\r\n");

        let mut payload = [0_u8; 4];
        stream.read_exact(&mut payload).await.expect("read payload");
        stream.write_all(&payload).await.expect("write payload");
    });

    ready_rx.await.expect("trojan tls server ready");
    task
}
