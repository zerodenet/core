use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{watch, Notify};
use zero_config::RuntimeConfig;

#[cfg(feature = "authenticated-quic-inbound-runtime")]
use super::{run_quic_listener_loop, QuicListenerLoopRequest};
#[cfg(feature = "transport_quic")]
use super::{run_quic_stream_listener_loop, QuicStreamListenerLoopRequest};
use super::{run_tcp_listener_loop, TcpListenerLoopRequest};
use crate::runtime::route_runtime::{InboundRouteRuntimeFactory, SharedIngressRuntimeServices};

fn test_runtime_factory(inbound_tag: &str) -> InboundRouteRuntimeFactory {
    let config =
        RuntimeConfig::parse(r#"{ "route": { "rules": [], "final": { "type": "direct" } } }"#)
            .expect("minimal runtime config");
    let proxy = crate::runtime::Proxy::new(config).expect("minimal proxy");
    InboundRouteRuntimeFactory::new(
        SharedIngressRuntimeServices::new(proxy.tcp_runtime_services()),
        inbound_tag.to_owned(),
    )
}

#[tokio::test]
async fn tcp_listener_accepts_connection_and_stops_on_shutdown() {
    let listener = zero_platform_tokio::TokioListener::bind("127.0.0.1:0")
        .await
        .expect("test listener");
    let listen_addr = listener.local_addr().expect("listener address");
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let accepted = Arc::new(Notify::new());
    let observations = Arc::new(Mutex::new(Vec::new()));
    let task = {
        let accepted = accepted.clone();
        let observations = observations.clone();
        tokio::spawn(async move {
            run_tcp_listener_loop(TcpListenerLoopRequest {
                runtime_factory: test_runtime_factory("listener-test"),
                protocol_name: "test",
                listener,
                shutdown: shutdown_rx,
                handler: move |runtime: crate::runtime::route_runtime::InboundRouteRuntime, _| {
                    let accepted = accepted.clone();
                    let observations = observations.clone();
                    async move {
                        observations
                            .lock()
                            .expect("observations lock")
                            .push((runtime.inbound_tag().to_owned(), runtime.source_addr()));
                        accepted.notify_one();
                    }
                },
            })
            .await
        })
    };

    let client = tokio::net::TcpStream::connect(listen_addr)
        .await
        .expect("connect listener");
    let client_addr = client.local_addr().expect("client address");
    tokio::time::timeout(Duration::from_secs(2), accepted.notified())
        .await
        .expect("handler was not invoked");

    shutdown_tx.send(true).expect("send listener shutdown");
    let result = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("listener did not stop")
        .expect("listener task panicked");
    result.expect("listener loop failed");
    drop(client);

    let observations = observations.lock().expect("observations lock");
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].0, "listener-test");
    assert_eq!(observations[0].1, Some(client_addr));
}

#[cfg(feature = "transport_quic")]
struct TestQuicMaterial {
    _directory: tempfile::TempDir,
    cert_path: std::path::PathBuf,
    key_path: std::path::PathBuf,
    certificate: rustls::pki_types::CertificateDer<'static>,
}

#[cfg(feature = "transport_quic")]
fn test_quic_material() -> TestQuicMaterial {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
        .expect("generate QUIC certificate");
    let directory = tempfile::tempdir().expect("create QUIC certificate directory");
    let cert_path = directory.path().join("server.crt");
    let key_path = directory.path().join("server.key");
    std::fs::write(&cert_path, certified.cert.pem()).expect("write QUIC certificate");
    std::fs::write(&key_path, certified.signing_key.serialize_pem())
        .expect("write QUIC private key");
    TestQuicMaterial {
        _directory: directory,
        cert_path,
        key_path,
        certificate: certified.cert.der().clone(),
    }
}

#[cfg(feature = "transport_quic")]
fn quic_client_endpoint(
    certificate: rustls::pki_types::CertificateDer<'static>,
    alpn: &[u8],
) -> quinn::Endpoint {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).expect("trust QUIC test certificate");
    let mut tls = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![alpn.to_vec()];
    let crypto =
        quinn::crypto::rustls::QuicClientConfig::try_from(tls).expect("build QUIC client crypto");
    let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().expect("client bind address"))
        .expect("create QUIC client endpoint");
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(crypto)));
    endpoint
}

#[cfg(feature = "transport_quic")]
async fn bind_quic_listener(
    material: &TestQuicMaterial,
) -> (zero_transport::quic::QuicInbound, std::net::SocketAddr) {
    let listener = zero_transport::quic::QuicInbound::bind(
        "127.0.0.1:0",
        material.cert_path.to_str().expect("certificate path"),
        material.key_path.to_str().expect("private key path"),
        None,
        &[b"zero-listener-test".to_vec()],
    )
    .await
    .expect("bind QUIC listener");
    let address = listener.local_addr().expect("QUIC listener address");
    (listener, address)
}

#[cfg(feature = "transport_quic")]
#[tokio::test]
async fn quic_stream_listener_survives_client_close_before_first_stream() {
    let material = test_quic_material();
    let (listener, listen_address) = bind_quic_listener(&material).await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let accepted = Arc::new(Notify::new());
    let task = {
        let accepted = accepted.clone();
        tokio::spawn(async move {
            run_quic_stream_listener_loop(QuicStreamListenerLoopRequest {
                runtime_factory: test_runtime_factory("quic-stream-test"),
                protocol_name: "test",
                listener,
                shutdown: shutdown_rx,
                handler: move |_, _| {
                    let accepted = accepted.clone();
                    async move { accepted.notify_one() }
                },
            })
            .await
        })
    };

    let client = quic_client_endpoint(material.certificate.clone(), b"zero-listener-test");
    let abandoned = client
        .connect(listen_address, "localhost")
        .expect("start abandoned QUIC connection")
        .await
        .expect("complete abandoned QUIC handshake");
    abandoned.close(quinn::VarInt::from_u32(0), b"close-before-stream");

    let connection = client
        .connect(listen_address, "localhost")
        .expect("start valid QUIC connection")
        .await
        .expect("complete valid QUIC handshake");
    let (mut send, _receive) = connection.open_bi().await.expect("open QUIC stream");
    send.write_all(b"x").await.expect("announce QUIC stream");
    tokio::time::timeout(Duration::from_secs(2), accepted.notified())
        .await
        .expect("listener stopped after one client abandoned its connection");

    shutdown_tx.send(true).expect("send QUIC listener shutdown");
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("QUIC stream listener did not stop")
        .expect("QUIC stream listener task panicked")
        .expect("QUIC stream listener failed");
}

#[cfg(feature = "authenticated-quic-inbound-runtime")]
#[tokio::test]
async fn quic_connection_listener_survives_failed_client_handshake() {
    let material = test_quic_material();
    let (listener, listen_address) = bind_quic_listener(&material).await;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let accepted = Arc::new(Notify::new());
    let task = {
        let accepted = accepted.clone();
        tokio::spawn(async move {
            run_quic_listener_loop(QuicListenerLoopRequest {
                runtime_factory: test_runtime_factory("quic-connection-test"),
                protocol_name: "test",
                listener,
                shutdown: shutdown_rx,
                handler: move |_, _| {
                    let accepted = accepted.clone();
                    async move { accepted.notify_one() }
                },
            })
            .await
        })
    };

    let incompatible = quic_client_endpoint(material.certificate.clone(), b"wrong-alpn");
    let failed = incompatible
        .connect(listen_address, "localhost")
        .expect("start incompatible QUIC connection")
        .await;
    assert!(failed.is_err(), "incompatible ALPN must fail the handshake");

    let client = quic_client_endpoint(material.certificate.clone(), b"zero-listener-test");
    let _connection = client
        .connect(listen_address, "localhost")
        .expect("start valid QUIC connection")
        .await
        .expect("complete valid QUIC handshake");
    tokio::time::timeout(Duration::from_secs(2), accepted.notified())
        .await
        .expect("listener stopped after one client failed its handshake");

    shutdown_tx.send(true).expect("send QUIC listener shutdown");
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .expect("QUIC connection listener did not stop")
        .expect("QUIC connection listener task panicked")
        .expect("QUIC connection listener failed");
}
