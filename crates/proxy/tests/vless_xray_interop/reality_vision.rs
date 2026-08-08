use std::sync::Arc;

use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_rustls::{TlsAcceptor, TlsConnector};

const REALITY_PRIVATE_KEY: &str = "OKMOFBeltHBXaTQ8cIcsgabVQcqXeTB9Ih3lPtWMY04";
const REALITY_PUBLIC_KEY: &str = "9AwHi13y1rN6EWTSo8-HNCOhrzr251jNY7SSIxo0diA";
const REALITY_SHORT_ID: &str = "0123456789abcdef";
const REALITY_SERVER_NAME: &str = "www.cloudflare.com";

#[tokio::test]
#[ignore = "requires XRAY_BIN pointing to an Xray executable"]
async fn zero_vless_reality_vision_outbound_interops_with_xray() {
    init_logs("vless=debug");
    let material = TempMaterial::new("zero-xray-vless-reality-vision-out");
    let xray_port = free_port();
    let zero_socks_port = free_port();
    let echo_port = free_port();
    let payload = b"xray-reality-vision";
    let xray_config = material.path("xray-server.json");
    std::fs::write(
        &xray_config,
        xray_vless_reality_vision_inbound_config(xray_port),
    )
    .expect("write xray config");
    let Some(xray_bin) = require_env("XRAY_BIN") else {
        return;
    };
    let mut xray = XrayProcess::start(xray_bin, &xray_config, &material);
    wait_for_listener(xray_port).await;
    let zero = spawn_vless_reality_vision_outbound(zero_socks_port, xray_port).await;
    let echo = spawn_tcp_echo(echo_port, payload.len()).await;

    let echoed = timeout(
        Duration::from_secs(10),
        socks5_tcp_echo_once(zero_socks_port, echo_port, payload),
    )
    .await
    .unwrap_or_else(|error| panic!("Reality+Vision timed out: {error}; logs={}", xray.logs()))
    .unwrap_or_else(|error| panic!("Reality+Vision failed: {error:?}; logs={}", xray.logs()));
    assert_eq!(echoed, payload, "xray logs={}", xray.logs());
    wait_for_echo(echo).await;

    let tls_echo_port = free_port();
    let tls_payload = b"xray-reality-vision-direct";
    let (tls_echo, cert) = spawn_tls_echo(tls_echo_port, tls_payload.len()).await;
    let tls_echoed = timeout(
        Duration::from_secs(10),
        socks5_tls_echo(zero_socks_port, tls_echo_port, tls_payload, cert),
    )
    .await
    .unwrap_or_else(|error| {
        panic!(
            "Reality+Vision direct mode timed out: {error}; logs={}",
            xray.logs()
        )
    })
    .unwrap_or_else(|error| {
        panic!(
            "Reality+Vision direct mode failed: {error}; logs={}",
            xray.logs()
        )
    });
    assert_eq!(tls_echoed, tls_payload, "xray logs={}", xray.logs());

    shutdown_zero(zero).await;
    wait_for_echo(tls_echo).await;
    xray.kill();
}

async fn spawn_vless_reality_vision_outbound(
    zero_socks_port: u16,
    server_port: u16,
) -> zero_proxy::RunningProxy {
    let config = RuntimeConfig::parse(&format!(
        r#"{{
            "inbounds": [{{
                "tag": "socks-in",
                "listen": {{ "address": "127.0.0.1", "port": {zero_socks_port} }},
                "protocol": {{ "type": "socks5" }}
            }}],
            "outbounds": [{{
                "tag": "vless-out",
                "protocol": {{
                    "type": "vless",
                    "server": "127.0.0.1",
                    "port": {server_port},
                    "id": "{USER_ID}",
                    "flow": "xtls-rprx-vision",
                    "reality": {{
                        "public_key": "{REALITY_PUBLIC_KEY}",
                        "short_id": "{REALITY_SHORT_ID}",
                        "server_name": "{REALITY_SERVER_NAME}",
                        "client_fingerprint": "chrome"
                    }}
                }}
            }}],
            "route": {{ "rules": [], "final": {{ "type": "route", "outbound": "vless-out" }} }}
        }}"#
    ))
    .expect("parse Zero Reality+Vision config");
    let zero = spawn_engine(Engine::new(config).expect("build Zero Reality+Vision engine"));
    wait_for_listener(zero_socks_port).await;
    zero
}

async fn spawn_tls_echo(
    port: u16,
    payload_len: usize,
) -> (
    tokio::task::JoinHandle<()>,
    rustls::pki_types::CertificateDer<'static>,
) {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate TLS echo certificate");
    let cert = certified.cert.der().clone();
    let key = rustls::pki_types::PrivateKeyDer::from(rustls::pki_types::PrivatePkcs8KeyDer::from(
        certified.signing_key.serialize_der(),
    ));
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .expect("build TLS echo config");
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind TLS echo");
        let _ = ready_tx.send(());
        let (stream, _) = listener.accept().await.expect("accept TLS echo");
        let mut stream = acceptor.accept(stream).await.expect("accept TLS handshake");
        let mut payload = vec![0_u8; payload_len];
        stream
            .read_exact(&mut payload)
            .await
            .expect("read TLS payload");
        stream.write_all(&payload).await.expect("write TLS payload");
    });
    ready_rx.await.expect("TLS echo ready");
    (task, cert)
}

async fn socks5_tls_echo(
    proxy_port: u16,
    target_port: u16,
    payload: &[u8],
    cert: rustls::pki_types::CertificateDer<'static>,
) -> std::io::Result<Vec<u8>> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", proxy_port)).await?;
    stream.write_all(&[0x05, 0x01, 0x00]).await?;
    let mut auth = [0_u8; 2];
    stream.read_exact(&mut auth).await?;
    if auth != [0x05, 0x00] {
        return Err(std::io::Error::other("SOCKS authentication failed"));
    }
    let mut request = vec![0x05, 0x01, 0x00, 0x01, 127, 0, 0, 1];
    request.extend_from_slice(&target_port.to_be_bytes());
    stream.write_all(&request).await?;
    let mut response = [0_u8; 10];
    stream.read_exact(&mut response).await?;
    if response[1] != 0 {
        return Err(std::io::Error::other("SOCKS connect failed"));
    }

    let mut roots = rustls::RootCertStore::empty();
    roots
        .add(cert)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("valid TLS server name")
        .to_owned();
    let mut stream = connector.connect(server_name, stream).await?;
    stream.write_all(payload).await?;
    let mut echoed = vec![0_u8; payload.len()];
    stream.read_exact(&mut echoed).await?;
    Ok(echoed)
}

fn xray_vless_reality_vision_inbound_config(port: u16) -> String {
    format!(
        r#"{{
            "log": {{ "loglevel": "debug" }},
            "inbounds": [{{
                "listen": "127.0.0.1",
                "port": {port},
                "protocol": "vless",
                "settings": {{
                    "clients": [{{
                        "id": "{USER_ID}",
                        "flow": "xtls-rprx-vision"
                    }}],
                    "decryption": "none"
                }},
                "streamSettings": {{
                    "network": "tcp",
                    "security": "reality",
                    "realitySettings": {{
                        "show": false,
                        "dest": "{REALITY_SERVER_NAME}:443",
                        "xver": 0,
                        "serverNames": ["{REALITY_SERVER_NAME}"],
                        "privateKey": "{REALITY_PRIVATE_KEY}",
                        "shortIds": ["{REALITY_SHORT_ID}"]
                    }}
                }}
            }}],
            "outbounds": [{{ "protocol": "freedom", "settings": {{}} }}]
        }}"#
    )
}
