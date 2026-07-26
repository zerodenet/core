#![cfg(feature = "vless")]

mod support;

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::TlsConnector;
use vless::parse_uuid;
use zero_api::{event_type, EventFilter, EventSource};
use zero_config::RuntimeConfig;
use zero_proxy::Proxy as Engine;

use support::{free_port, spawn_engine, wait_for, wait_for_listener};

const USER_ID: &str = "11111111-2222-3333-4444-555555555555";
static NEXT_TLS_DIR: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "vless")]
#[path = "vless/relays_tcp_through_vless_chained_outbound.rs"]
mod relays_tcp_through_vless_chained_outbound;
#[path = "vless/relays_tcp_through_vless_direct_outbound_and_records_principal.rs"]
mod relays_tcp_through_vless_direct_outbound_and_records_principal;
#[cfg(all(feature = "socks5", feature = "vless"))]
#[path = "vless/relays_tcp_through_vless_grpc_chained_outbound.rs"]
mod relays_tcp_through_vless_grpc_chained_outbound;
#[cfg(feature = "vless")]
#[path = "vless/relays_tcp_through_vless_reality_xray.rs"]
mod relays_tcp_through_vless_reality_xray;
#[cfg(feature = "vless")]
#[path = "vless/relays_tcp_through_vless_reality_zero_inbound.rs"]
mod relays_tcp_through_vless_reality_zero_inbound;
#[cfg(feature = "vless")]
#[path = "vless/relays_tcp_through_vless_tls_chained_outbound.rs"]
mod relays_tcp_through_vless_tls_chained_outbound;
#[path = "vless/relays_tcp_through_vless_tls_direct_outbound.rs"]
mod relays_tcp_through_vless_tls_direct_outbound;
#[cfg(feature = "vless")]
#[path = "vless/relays_tcp_through_vless_ws_chained_outbound.rs"]
mod relays_tcp_through_vless_ws_chained_outbound;
#[path = "vless/relays_tcp_through_vless_ws_inbound.rs"]
mod relays_tcp_through_vless_ws_inbound;
#[cfg(feature = "vless")]
#[path = "vless/relays_tcp_through_vless_wss_chained_outbound.rs"]
mod relays_tcp_through_vless_wss_chained_outbound;
#[path = "vless/vless_device_limit_enforces_unique_source_ips.rs"]
mod vless_device_limit_enforces_unique_source_ips;
#[cfg(feature = "vless")]
#[path = "vless/vless_outbound_tls_insecure_skip_verification.rs"]
mod vless_outbound_tls_insecure_skip_verification;
#[path = "vless/vless_principal_cancellation_closes_active_tcp.rs"]
mod vless_principal_cancellation_closes_active_tcp;
#[path = "vless/vless_quota_exhaustion_closes_principal.rs"]
mod vless_quota_exhaustion_closes_principal;
#[path = "vless/vless_tls_with_alpn_config.rs"]
mod vless_tls_with_alpn_config;
#[path = "vless/vless_user_reload_updates_live_listener.rs"]
mod vless_user_reload_updates_live_listener;

fn vless_request_for_ipv4(id: &str, address: [u8; 4], port: u16) -> Vec<u8> {
    let id = parse_uuid(id).expect("uuid");
    let mut request = vec![0x00];
    request.extend_from_slice(&id);
    request.extend_from_slice(&[
        0x00, // addon length
        0x01, // tcp command
        ((port >> 8) & 0xff) as u8,
        (port & 0xff) as u8,
        0x01, // ipv4
    ]);
    request.extend_from_slice(&address);
    request
}

async fn spawn_echo_server(port: u16) -> tokio::task::JoinHandle<()> {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        let listener = TcpListener::bind(("127.0.0.1", port))
            .await
            .expect("bind echo");
        let _ = ready_tx.send(());
        let (mut stream, _) = listener.accept().await.expect("accept echo");
        let mut buf = [0_u8; 4];
        stream.read_exact(&mut buf).await.expect("read echo");
        stream.write_all(&buf).await.expect("write echo");
    });
    ready_rx.await.expect("echo server ready");
    task
}

async fn connect_tls_client(
    port: u16,
    cert: rustls::pki_types::CertificateDer<'static>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trust test cert");
    let config = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(config));
    let server_name = rustls::pki_types::ServerName::try_from("localhost")
        .expect("server name")
        .to_owned();
    let stream = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect proxy");

    connector
        .connect(server_name, stream)
        .await
        .expect("tls handshake")
}

struct TestTlsMaterial {
    dir: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_der: rustls::pki_types::CertificateDer<'static>,
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
        "zero-vless-tls-{}-{}-{}",
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
        cert_der: certified.cert.der().clone(),
    }
}

fn escape_json_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "\\\\")
}
